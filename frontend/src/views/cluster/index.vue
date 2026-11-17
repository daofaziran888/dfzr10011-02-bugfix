<template>
  <section class="page" data-module="cluster">
    <header class="page-head">
      <div>
        <h2>采集节点集群管理</h2>
        <p class="page-desc">维护采集集群的节点配置、资源规格与健康监控，为采集节点集群管理提供可审计的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记集群节点</button>
        <button class="btn" type="button" @click="exportRows">导出采集节点集群清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section class="policy-bar">
      <span>冲突终态策略：<strong>{{ policy.策略名称 || '未读取' }}</strong></span>
      <label class="filter-item">
        <span>恢复 / 离线回执冲突时</span>
        <select v-model="selectedPolicy" @change="applyPolicy">
          <option v-for="item in policy.候选 || []" :key="item.policy" :value="item.policy">
            {{ item.名称 }}
          </option>
        </select>
      </label>
      <span class="hint">恢复终态优先：详情显示恢复、候选暂保留并附离线待清理；离线终态优先：详情显示离线、候选立即移除并附恢复待办。</span>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>节点可用性</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column" :class="{ 'cell-link': column === '节点编号' }" @click="column === '节点编号' && openDetail(row)">
            {{ row[column] ?? '—' }}
          </td>
          <td>{{ availabilityLabel(row) }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="openDetail(row)">详情</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无采集节点集群数据，可先登记集群节点</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集节点集群记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section v-if="detail" class="detail-panel">
      <header class="detail-head">
        <h3>节点详情 · {{ detail.节点编号 }}</h3>
        <button class="btn ghost" type="button" @click="detail = null">收起</button>
      </header>
      <dl class="detail-grid">
        <div><dt>节点状态</dt><dd>{{ detail.节点状态 }}</dd></div>
        <div><dt>可用性结果</dt><dd>{{ detail.availability?.可用性状态 }}</dd></div>
        <div><dt>候选范围</dt><dd>{{ detail.availability?.candidate ? '保留在分片候选' : '已移出分片候选' }}</dd></div>
        <div><dt>终态</dt><dd>{{ detail.availability?.终态 }}</dd></div>
        <div><dt>终态策略</dt><dd>{{ detail.availability?.策略名称 }}</dd></div>
        <div><dt>下线依据</dt><dd>{{ detail.availability?.下线依据 || '—' }}</dd></div>
        <div><dt>恢复依据</dt><dd>{{ detail.availability?.恢复依据 || '—' }}</dd></div>
        <div><dt>结果版本</dt><dd>{{ detail.版本 }}</dd></div>
      </dl>
      <p class="detail-note">{{ detail.availability?.候选说明 }}</p>

      <div class="receipt-bar">
        <label class="filter-item">
          <span>回执事件</span>
          <select v-model="receiptForm.event">
            <option value="恢复">恢复</option>
            <option value="离线">离线</option>
            <option value="移除">移除</option>
          </select>
        </label>
        <label class="filter-item">
          <span>回执编号</span>
          <input v-model="receiptForm.receipt_id" placeholder="例如 OFF-100" />
        </label>
        <label class="filter-item">
          <span>回执时间</span>
          <input v-model="receiptForm.occurred_at" placeholder="2026-09-11T08:00:00" />
        </label>
        <button class="btn primary" type="button" @click="submitReceipt">登记回执</button>
        <label class="filter-item">
          <span>心跳编号</span>
          <input v-model="heartbeatForm.receipt_id" placeholder="例如 HB-100" />
        </label>
        <button class="btn" type="button" @click="submitHeartbeat">登记心跳</button>
      </div>

      <h4>生命周期记录（回执追溯）</h4>
      <table class="data-table compact">
        <thead>
          <tr><th>回执编号</th><th>事件</th><th>原状态</th><th>终状态</th><th>回执时间</th><th>接收序号</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in detail.生命周期记录" :key="String(item.id)">
            <td>{{ item.receipt_id }}</td>
            <td>{{ item.事件 }}</td>
            <td>{{ item.from_status || '—' }}</td>
            <td>{{ item.to_status || '—' }}</td>
            <td>{{ item.回执时间 }}</td>
            <td>{{ item.接收序号 }}</td>
          </tr>
          <tr v-if="!detail.生命周期记录?.length"><td colspan="6" class="empty-state">暂无生命周期回执</td></tr>
        </tbody>
      </table>

      <h4>节点心跳</h4>
      <table class="data-table compact">
        <thead>
          <tr><th>回执编号</th><th>心跳时间</th><th>状态</th><th>接收序号</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in detail.心跳记录" :key="String(item.id)">
            <td>{{ item.receipt_id }}</td>
            <td>{{ item.心跳时间 }}</td>
            <td>{{ item.状态 }}</td>
            <td>{{ item.接收序号 }}</td>
          </tr>
          <tr v-if="!detail.心跳记录?.length"><td colspan="4" class="empty-state">暂无心跳记录</td></tr>
        </tbody>
      </table>

      <h4>修复节点下线后状态复核清单</h4>
      <table class="data-table compact">
        <thead>
          <tr><th>类型</th><th>回执编号</th><th>说明</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in detail.availability?.复核清单 || []" :key="`${item.类型}-${item.receipt_id}`">
            <td>{{ item.类型 }}</td>
            <td>{{ item.receipt_id }}</td>
            <td>{{ item.说明 }}</td>
            <td>{{ item.状态 }}</td>
          </tr>
          <tr v-if="!detail.availability?.复核清单?.length"><td colspan="4" class="empty-state">暂无待复核事项</td></tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { fetchJson, request } from '@/api/client'

type Row = Record<string, string | number | null>
type Detail = Row & {
  availability?: Record<string, any>
  生命周期记录?: Record<string, any>[]
  心跳记录?: Record<string, any>[]
}
type Policy = { policy?: string; 策略名称?: string; 候选?: { policy: string; 名称: string }[] }
type Availability = Record<string, any>

const ENDPOINT = '/api/cluster'
const columns = ["节点编号", "节点地址", "节点角色", "CPU核心", "内存容量", "网络带宽", "心跳时间", "节点状态"]
const actions = ["加入集群", "下线节点", "移除节点"]
const statuses = ["待加入", "运行中", "已离线", "已移除"]
const stats = ref([{"label": "集群节点总数", "value": 0}, {"label": "运行中节点", "value": 0}, {"label": "已离线节点", "value": 0}])

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const detail = ref<Detail | null>(null)
const availability = ref<Record<string, Availability>>({})
const policy = ref<Policy>({})
const selectedPolicy = ref('')
const receiptForm = ref({ event: '离线', receipt_id: '', occurred_at: '' })
const heartbeatForm = ref({ receipt_id: '' })

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '集群节点登记入口尚未接入审批流'
}

function availabilityLabel(row: Row) {
  const item = availability.value[String(row.节点编号)]
  if (!item) return '—'
  return `${item.可用性状态}${item.candidate ? ' · 候选' : ''}`
}

async function openDetail(row: Row) {
  errorMessage.value = ''
  try {
    const payload = await fetchJson<Detail>(`${ENDPOINT}/by-code/${encodeURIComponent(String(row.节点编号))}`)
    detail.value = payload
    await loadAvailability()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '节点详情读取失败'
  }
}

async function loadAvailability() {
  const payload = await fetchJson<{ items: Availability[]; policy: Policy }>(`${ENDPOINT}/availability`)
  const map: Record<string, Availability> = {}
  for (const item of payload.items || []) {
    map[String(item.节点编号)] = item
  }
  availability.value = map
  policy.value = payload.policy || {}
  selectedPolicy.value = payload.policy?.policy || ''
}

async function submitReceipt() {
  if (!detail.value) return
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${detail.value.id}/receipts`, {
      method: 'POST',
      body: JSON.stringify({
        event: receiptForm.value.event,
        receipt_id: receiptForm.value.receipt_id,
        occurred_at: receiptForm.value.occurred_at || undefined,
      }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '生命周期回执登记未生效')
    }
    receiptForm.value.receipt_id = ''
    receiptForm.value.occurred_at = ''
    await reload()
    await openDetail(detail.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生命周期回执登记失败'
  }
}

async function submitHeartbeat() {
  if (!detail.value) return
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${detail.value.id}/heartbeats`, {
      method: 'POST',
      body: JSON.stringify({
        receipt_id: heartbeatForm.value.receipt_id,
        beat_time: new Date().toISOString(),
        status: '在线',
      }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '心跳登记未生效')
    }
    heartbeatForm.value.receipt_id = ''
    await reload()
    await openDetail(detail.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '心跳登记失败'
  }
}

async function applyPolicy() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/policy`, {
      method: 'POST',
      body: JSON.stringify({ policy: selectedPolicy.value }),
    })
    if (!response.ok) {
      throw new Error('终态策略切换未生效')
    }
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '终态策略切换未生效')
    }
    await reload()
    if (detail.value) {
      await openDetail(detail.value)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '终态策略切换失败'
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
      throw new Error('采集节点集群动作未生效，请稍后重试')
    }
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '采集节点集群动作未生效')
    }
    await reload()
    if (detail.value) {
      await openDetail(row)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集节点集群操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('集群节点列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await loadAvailability()
    stats.value[0].value = total.value
    stats.value[1].value = rows.value.filter((row) => row.节点状态 === '运行中').length
    stats.value[2].value = rows.value.filter((row) => row.节点状态 === '已离线').length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集节点集群列表读取失败'
  }
}

onMounted(reload)
</script>
