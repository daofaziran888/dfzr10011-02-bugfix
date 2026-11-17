<template>
  <section class="page" data-module="shard">
    <header class="page-head">
      <div>
        <h2>任务分片调度管理</h2>
        <p class="page-desc">维护任务分片的范围划分、节点分配与重试控制，为任务分片调度提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记分片任务</button>
        <button class="btn" type="button" @click="exportRows">导出任务分片调度清单</button>
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
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>
            {{ row.节点可用性 ? `${row.节点可用性}${row.节点候选 ? ' · 候选' : ' · 已移出候选'}` : '—' }}
            <span v-if="row.下线依据" class="hint">（下线依据 {{ row.下线依据 }}）</span>
          </td>
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
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无任务分片调度数据，可先登记分片任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条任务分片调度记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section class="detail-panel">
      <header class="detail-head">
        <h3>修复节点下线后状态复核</h3>
        <button class="btn" type="button" @click="loadRecheck">刷新复核清单</button>
      </header>
      <div class="stat-row">
        <article class="stat-card"><span class="stat-label">可用性结果</span><strong class="stat-value">{{ recheck.summary?.节点总数 ?? 0 }}</strong></article>
        <article class="stat-card"><span class="stat-label">候选节点</span><strong class="stat-value">{{ recheck.summary?.候选节点 ?? 0 }}</strong></article>
        <article class="stat-card"><span class="stat-label">已移出候选</span><strong class="stat-value">{{ recheck.summary?.已移出候选 ?? 0 }}</strong></article>
        <article class="stat-card"><span class="stat-label">复核项</span><strong class="stat-value">{{ recheck.summary?.复核项 ?? 0 }}</strong></article>
      </div>
      <p class="detail-note">终态策略：{{ recheck.policy?.策略名称 }} · 候选范围与节点详情读取同一份下线依据。</p>

      <h4>节点可用性结果</h4>
      <table class="data-table compact">
        <thead>
          <tr><th>节点编号</th><th>可用性</th><th>终态</th><th>候选</th><th>下线依据</th><th>恢复依据</th><th>结果版本</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in recheck.availability || []" :key="item.节点编号">
            <td>{{ item.节点编号 }}</td>
            <td>{{ item.可用性状态 }}</td>
            <td>{{ item.终态 }}</td>
            <td>{{ item.candidate ? '保留' : '已移出' }}</td>
            <td>{{ item.下线依据 || '—' }}</td>
            <td>{{ item.恢复依据 || '—' }}</td>
            <td>{{ item.version }}</td>
          </tr>
          <tr v-if="!recheck.availability?.length"><td colspan="7" class="empty-state">暂无节点可用性结果</td></tr>
        </tbody>
      </table>

      <h4>复核清单</h4>
      <table class="data-table compact">
        <thead>
          <tr><th>类型</th><th>节点编号</th><th>回执编号</th><th>回执时间</th><th>说明</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in recheck.recheck || []" :key="`${item.类型}-${item.receipt_id}-${item.节点编号}`">
            <td>{{ item.类型 }}</td>
            <td>{{ item.节点编号 }}</td>
            <td>{{ item.receipt_id }}</td>
            <td>{{ item.回执时间 }}</td>
            <td>{{ item.说明 }}</td>
            <td>{{ item.状态 }}</td>
          </tr>
          <tr v-if="!recheck.recheck?.length"><td colspan="6" class="empty-state">暂无待复核事项</td></tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { fetchJson, request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/shard'
const columns = ["分片编号", "采集计划", "分片范围", "分配节点", "开始时间", "结束时间", "重试次数", "分片状态"]
const actions = ["分配节点", "重试分片", "标记完成"]
const statuses = ["待分配", "执行中", "已完成", "执行失败"]
const stats = ref([{"label": "分片任务总数", "value": 0}, {"label": "执行中分片", "value": 0}, {"label": "已完成分片", "value": 0}])
const recheck = ref<Record<string, any>>({})

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '分片任务登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('任务分片调度动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务分片调度操作失败'
  }
}

async function loadRecheck() {
  try {
    recheck.value = await fetchJson<Record<string, any>>(`${ENDPOINT}/recheck`)
  } catch (error) {
    recheck.value = {}
    errorMessage.value = error instanceof Error ? error.message : '复核清单读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('分片任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    stats.value[0].value = total.value
    stats.value[1].value = rows.value.filter((row) => row.status === '执行中').length
    stats.value[2].value = rows.value.filter((row) => row.status === '已完成').length
    await loadRecheck()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务分片调度列表读取失败'
  }
}

onMounted(async () => {
  await reload()
  await loadRecheck()
})
</script>
