import { expect, test } from '@playwright/test'

const BATCH_API = '/api/archive/batches'

test('归档批次数据集：生成、校验、清理状态与部分失败补做', async ({ page, request }) => {
  test.setTimeout(120_000)
  const browserErrors: string[] = []
  page.on('pageerror', (error) => browserErrors.push(error.message))
  page.on('response', (response) => {
    if (response.url().includes('/api/') && response.status() >= 400) {
      browserErrors.push(`${response.status()} ${response.url()}`)
    }
  })

  // 生成一个带失败分区的批次，成功分区归档次数必须保持 1。
  const created = await request.post(BATCH_API, {
    data: { strategy: 2, data_end: '2026-09-30', partition_count: 3, fail_count: 1 },
  })
  expect(created.ok()).toBeTruthy()
  const batch = (await created.json()).entry
  expect(batch['批次状态']).toBe('部分失败')
  expect(batch['校验摘要']).toBeFalsy()

  // 缺少校验摘要时不能落地，也不能进入可清理范围。
  const invalidLanding = await request.post(`${BATCH_API}/bulk`, {
    data: { action: '落地存储', ids: [batch.id] },
  })
  expect((await invalidLanding.json()).results[0].ok).toBeFalsy()
  const invalidClean = await request.post(`${BATCH_API}/bulk`, {
    data: { action: '进入可清理', ids: [batch.id] },
  })
  expect((await invalidClean.json()).results[0].ok).toBeFalsy()

  // 补做失败分区：只有失败分区被重新归档。
  const retry = await request.post(`${BATCH_API}/bulk`, {
    data: { action: '补做失败分区', ids: [batch.id] },
  })
  expect((await retry.json()).results[0].ok).toBeTruthy()
  const detail = await (await request.get(`${BATCH_API}/${batch.id}`)).json()
  const counts = detail.partitions.map((part: Record<string, number | string>) => ({
    code: String(part['分区编号']),
    status: String(part['归档状态']),
    times: Number(part['归档次数']),
  }))
  expect(counts.filter((item: { status: string }) => item.status === '已归档' && item.times === 1)).toHaveLength(2)
  expect(counts.filter((item: { times: number }) => item.times === 2)).toHaveLength(1)

  // 校验通过后写入；数据在保留期内时不可清理。
  const verified = await request.post(`${BATCH_API}/bulk`, {
    data: { action: '校验归档文件', ids: [batch.id] },
  })
  expect((await verified.json()).results[0].ok).toBeTruthy()
  const afterVerify = await (await request.get(`${BATCH_API}/${batch.id}`)).json()
  expect(afterVerify['校验状态']).toBe('校验通过')
  expect(afterVerify['写入状态']).toBe('已写入')
  expect(afterVerify['清理状态']).toBe('保留期内')

  // 归档落地清单与批次一一对应，落地路径与校验摘要一致。
  const landing = await request.post(`${BATCH_API}/bulk`, {
    data: { action: '落地存储', ids: [batch.id] },
  })
  expect((await landing.json()).results[0].ok).toBeTruthy()
  const landingRow = await (await request.get(`${BATCH_API}/${batch.id}/landing`)).json()
  expect(landingRow['批次编号']).toBe(batch['批次编号'])
  expect(landingRow['校验摘要']).toBe(afterVerify['校验摘要'])
  expect(landingRow['落地路径']).toContain(batch['批次编号'])

  // 重复送交同一批次数据集只保留一个批次编号。
  const duplicate = await (await request.post(BATCH_API, { data: { strategy: 2, data_end: '2026-09-30' } })).json()
  expect(duplicate.duplicated).toBeTruthy()
  expect(duplicate.entry['批次编号']).toBe(batch['批次编号'])

  // 整组提交失败时回滚，批次保持上一版本。
  const failure = await (await request.post(`${BATCH_API}/bulk`, {
    data: { action: '清理回执', ids: [batch.id], receipt_no: 'CLR-E2E', simulate_commit_failure: true },
  })).json()
  expect(failure.ok).toBeFalsy()
  expect(failure.rolled_back).toBeTruthy()
  const afterRollback = await (await request.get(`${BATCH_API}/${batch.id}`)).json()
  expect(afterRollback['回执状态']).toBe('待回执')

  // 页面可列出批次、分区、校验摘要和清理状态。
  await page.goto('/archive')
  const row = page.locator(`[data-testid="batch-row-${batch.id}"]`)
  await expect(row).toBeVisible()
  await expect(row).toHaveAttribute('data-cleanup-status', '保留期内')
  await row.locator(`[data-testid="batch-detail-${batch.id}"]`).click()
  await expect(page.getByTestId('batch-detail')).toBeVisible()
  await expect(page.locator(`[data-testid="partition-row-${batch['批次编号']}-P01"]`)).toBeVisible()

  expect(browserErrors).toEqual([])
})
