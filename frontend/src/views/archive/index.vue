<template>
  <section class="page" data-module="archive">
    <header class="page-head">
      <div>
        <h2>历史归档管理</h2>
        <p class="page-desc">管理采集历史数据的归档周期、格式和检索策略。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记归档策略</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出历史归档清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无历史归档数据，可先登记归档策略</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记归档策略</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <section class="batch-panel" data-testid="archive-batch-panel">
      <header class="batch-head">
        <div>
          <h3>归档批次数据集</h3>
          <p class="page-desc">按归档策略分批生成，校验通过后才写入；归档周期、格式与保留年限共同决定可清理范围。</p>
        </div>
        <div class="page-actions">
          <select v-model.number="selectedStrategy" data-testid="batch-strategy" aria-label="归档策略">
            <option :value="0" disabled>选择归档策略</option>
            <option v-for="strategy in strategyOptions" :key="String(strategy.id)" :value="strategy.id">
              {{ strategy['策略编号'] }}
            </option>
          </select>
          <input v-model="batchDataEnd" type="date" data-testid="batch-data-end" aria-label="数据截止日期" />
          <button class="btn primary" type="button" data-testid="generate-batch" :disabled="generating" @click="generateBatch">
            {{ generating ? '生成中…' : '分批生成归档批次' }}
          </button>
        </div>
      </header>

      <div class="stat-row">
        <article v-for="metric in batchMetrics" :key="metric.label" class="stat-card" data-testid="batch-metric">
          <span class="stat-label">{{ metric.label }}</span>
          <strong class="stat-value">{{ metric.value }}</strong>
        </article>
      </div>

      <div class="page-actions batch-toolbar">
        <label class="batch-check"><input v-model="selectAll" type="checkbox" data-testid="batch-select-all" /> 全选</label>
        <input v-model="receiptNo" data-testid="batch-receipt-no" placeholder="清理回执编号" />
        <button v-for="action in bulkActions" :key="action.key" class="btn" type="button"
          :data-testid="`batch-bulk-${action.key}`" :disabled="bulking" @click="runBulk(action.key)">
          {{ action.label }}
        </button>
      </div>

      <p v-if="batchMessage" class="batch-message" :data-testid="`batch-message-${batchMessageKind}`">{{ batchMessage }}</p>

      <table class="data-table" data-testid="batch-table">
        <thead>
          <tr>
            <th></th>
            <th>批次编号</th>
            <th>策略编号</th>
            <th>归档周期</th>
            <th>归档格式</th>
            <th>保留年限</th>
            <th>分区数</th>
            <th>校验摘要</th>
            <th>批次状态</th>
            <th>清理状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in batches" :key="String(batch.id)" :data-testid="`batch-row-${batch.id}`"
            :data-batch-status="String(batch['批次状态'])" :data-cleanup-status="String(batch['清理状态'])">
            <td><input v-model="selectedIds" type="checkbox" :value="Number(batch.id)" :data-testid="`batch-select-${batch.id}`" /></td>
            <td>{{ batch['批次编号'] }}</td>
            <td>{{ batch['策略编号'] }}</td>
            <td>{{ batch['归档周期'] }}</td>
            <td>{{ batch['归档格式'] }}</td>
            <td>{{ batch['保留年限'] }}</td>
            <td>{{ batch['分区数'] }}</td>
            <td class="digest-cell" :title="String(batch['校验摘要'] || '缺少校验摘要，不可清理')">
              {{ batch['校验摘要'] ? String(batch['校验摘要']).slice(0, 18) + '…' : '—' }}
            </td>
            <td>{{ batch['批次状态'] }}</td>
            <td>{{ batch['清理状态'] }}</td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`batch-detail-${batch.id}`" @click="toggleDetail(batch)">
                {{ expandedId === batch.id ? '收起分区' : '查看分区' }}
              </button>
            </td>
          </tr>
          <tr v-if="!batches.length">
            <td colspan="11" class="empty-state">暂无归档批次，请选择归档策略后分批生成</td>
          </tr>
        </tbody>
      </table>

      <div v-if="detail" class="batch-detail" data-testid="batch-detail">
        <h4>
          {{ detail['批次编号'] }} 分区明细
          <span class="batch-hint">可清理范围：{{ detail.cleanup_scope?.['可清理范围'] }} · 保留至 {{ detail.cleanup_scope?.['保留截止日期'] }}</span>
        </h4>
        <table class="data-table">
          <thead>
            <tr><th>分区编号</th><th>分区范围</th><th>归档状态</th><th>归档次数</th><th>校验摘要</th><th>归档路径</th><th>失败原因</th></tr>
          </thead>
          <tbody>
            <tr v-for="part in detail.partitions || []" :key="String(part.id)" :data-testid="`partition-row-${part['分区编号']}`" :data-archive-count="String(part['归档次数'])">
              <td>{{ part['分区编号'] }}</td>
              <td>{{ part['分区范围'] }}</td>
              <td>{{ part['归档状态'] }}</td>
              <td>{{ part['归档次数'] }}</td>
              <td class="digest-cell">{{ part['校验摘要'] ? String(part['校验摘要']).slice(0, 18) + '…' : '—' }}</td>
              <td>{{ part['归档路径'] }}</td>
              <td>{{ part['失败原因'] || '—' }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="detail.landing" class="batch-hint" data-testid="batch-landing">
          归档落地清单 {{ detail.landing['落地编号'] }} · 落地路径 {{ detail.landing['落地路径'] }} · 校验摘要一致：{{ detail.landing['校验摘要'] === detail['校验摘要'] ? '是' : '否' }}
        </p>
      </div>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条历史归档记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/archive'
const columns = ["策略编号", "数据来源", "归档周期", "归档格式", "保留年限", "压缩级别", "索引规则", "清理策略"]
const actions = ["提交校验", "启用配置", "停用配置"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const metricNames = ["归档策略总数", "待校验归档策略", "已启用归档策略"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

type BatchRow = Record<string, string | number | null>
type BatchDetail = BatchRow & {
  partitions?: BatchRow[]
  landing?: BatchRow | null
  cleanup_scope?: Record<string, string | number | string[]>
}

const BATCH_ENDPOINT = '/api/archive/batches'
const bulkActions = [
  { key: 'verify', label: '校验归档文件' },
  { key: 'retry', label: '补做失败分区' },
  { key: 'landing', label: '落地存储' },
  { key: 'mark_cleanable', label: '进入可清理' },
  { key: 'receipt', label: '清理回执' },
]
const actionNames: Record<string, string> = {
  verify: '校验归档文件',
  retry: '补做失败分区',
  landing: '落地存储',
  mark_cleanable: '进入可清理',
  receipt: '清理回执',
}

const strategyOptions = ref<Row[]>([])
const selectedStrategy = ref(0)
const batchDataEnd = ref('')
const batches = ref<BatchRow[]>([])
const batchMetrics = ref<{ label: string; value: number }[]>([])
const selectedIds = ref<number[]>([])
const receiptNo = ref('')
const generating = ref(false)
const bulking = ref(false)
const batchMessage = ref('')
const batchMessageKind = ref<'ok' | 'error'>('ok')
const expandedId = ref<number | null>(null)
const detail = ref<BatchDetail | null>(null)
const selectAll = computed({
  get: () => batches.value.length > 0 && selectedIds.value.length === batches.value.length,
  set: (checked: boolean) => {
    selectedIds.value = checked ? batches.value.map((batch) => Number(batch.id)) : []
  },
})

function setBatchMessage(text: string, kind: 'ok' | 'error') {
  batchMessage.value = text
  batchMessageKind.value = kind
}

async function loadBatchSummary() {
  try {
    const response = await request(`${BATCH_ENDPOINT}/summary`)
    if (!response.ok) return
    batchMetrics.value = (await response.json()).metrics ?? []
  } catch {
    batchMetrics.value = []
  }
}

async function loadBatches() {
  try {
    const response = await request(`${BATCH_ENDPOINT}?size=100`)
    if (!response.ok) throw new Error('归档批次列表读取失败')
    const payload = await response.json()
    batches.value = payload.items ?? []
    selectedIds.value = selectedIds.value.filter((id) => batches.value.some((batch) => Number(batch.id) === id))
    if (expandedId.value !== null && !batches.value.some((batch) => Number(batch.id) === expandedId.value)) {
      expandedId.value = null
      detail.value = null
    }
  } catch (error) {
    setBatchMessage(error instanceof Error ? error.message : '归档批次列表读取失败', 'error')
  }
}

async function loadStrategyOptions() {
  try {
    const response = await request(`${ENDPOINT}?size=200`)
    if (!response.ok) return
    const payload = await response.json()
    strategyOptions.value = payload.items ?? []
    if (!selectedStrategy.value && strategyOptions.value.length) {
      selectedStrategy.value = Number(strategyOptions.value[0].id)
    }
  } catch {
    strategyOptions.value = []
  }
}

async function refreshBatches() {
  await Promise.all([loadBatches(), loadBatchSummary()])
}

async function generateBatch() {
  if (!selectedStrategy.value) {
    setBatchMessage('请先选择归档策略', 'error')
    return
  }
  generating.value = true
  try {
    const response = await request(BATCH_ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({
        strategy: selectedStrategy.value,
        data_end: batchDataEnd.value || null,
      }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '归档批次生成失败')
    setBatchMessage(payload.message, 'ok')
    await refreshBatches()
    if (payload.entry) await toggleDetail(payload.entry, true)
  } catch (error) {
    setBatchMessage(error instanceof Error ? error.message : '归档批次生成失败', 'error')
  } finally {
    generating.value = false
  }
}

async function runBulk(action: string) {
  if (!selectedIds.value.length) {
    setBatchMessage('请先勾选归档批次', 'error')
    return
  }
  bulking.value = true
  try {
    const response = await request(`${BATCH_ENDPOINT}/bulk`, {
      method: 'POST',
      body: JSON.stringify({
        action: actionNames[action] ?? action,
        ids: selectedIds.value,
        receipt_no: receiptNo.value || null,
      }),
    })
    const payload = await response.json()
    const results: { ok: boolean; message: string; 批次编号?: string }[] = payload.results ?? []
    const failed = results.filter((item) => !item.ok)
    const lines = results.map((item) => `${item['批次编号'] || '批次'}：${item.ok ? '成功' : '失败'} - ${item.message}`)
    setBatchMessage([payload.message, ...lines].filter(Boolean).join('；'), failed.length ? 'error' : 'ok')
    await refreshBatches()
    if (expandedId.value !== null) await loadDetail(expandedId.value)
  } catch (error) {
    setBatchMessage(error instanceof Error ? error.message : '归档批次批量操作失败', 'error')
  } finally {
    bulking.value = false
  }
}

async function loadDetail(batchId: number) {
  try {
    const response = await request(`${BATCH_ENDPOINT}/${batchId}`)
    if (!response.ok) throw new Error('归档批次明细读取失败')
    detail.value = await response.json()
  } catch (error) {
    detail.value = null
    setBatchMessage(error instanceof Error ? error.message : '归档批次明细读取失败', 'error')
  }
}

async function toggleDetail(batch: BatchRow, forceOpen = false) {
  const batchId = Number(batch.id)
  if (!forceOpen && expandedId.value === batchId) {
    expandedId.value = null
    detail.value = null
    return
  }
  expandedId.value = batchId
  await loadDetail(batchId)
}

watch(expandedId, () => {
  if (expandedId.value === null) detail.value = null
})

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'archive.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('历史归档动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '历史归档操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('归档策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '历史归档列表读取失败'
  }
}

onMounted(() => {
  void loadStrategyOptions()
  void refreshBatches()
  void reload()
})
</script>
