/**
 * Free Lane Chat — 零依赖单端口 HTTP 服务
 *
 * 两套接口：
 *  - /api/*   本站网页自用（模型清单 / 车道体检 / 流式对话）
 *  - /v1/*    OpenAI 兼容接口，供第三方客户端、SDK、其他聊天应用直接接入
 *
 * 免费车道说明（如实告知用户）：
 *  - 推理走 Kilo AI 公共网关的免费模型池（isFree / pricing.prompt === "0"），无需账号或 Key；
 *  - OpenCode Zen 免密车道按出口 IP 计配额，实测返回 429 FreeUsageLimitError，仅在体检面板展示；
 *  - prompt、工具结果、图片会发往第三方上游；免费额度由上游单方面提供，随时可能限流、收费或下线；
 *  - /v1 的 API Key 只是占位符（上游本身免密），它不是访问控制，别把它当成安全措施。
 *
 * 分享网关兼容：
 *  平台在网关层要求 ?sharecode=<code> 或 __wb_rsc cookie。第三方客户端把 base_url 设为
 *  ".../v1?sharecode=XXX&" 后，字符串拼接型客户端会发出
 *  ".../v1?sharecode=XXX&/chat/completions"，此处把那个无值参数还原为真实路径。
 */
import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { Readable } from 'node:stream'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const PORT = Number(process.env.PORT || 3000)
const HOST = '0.0.0.0'
const PUBLIC_DIR = path.join(__dirname, 'public')
const BENCH_PATH = path.join(__dirname, 'bench.json')
const API_KEY = process.env.API_KEY || 'sk-free-lane'

const KILO_BASE = 'https://ai-gateway.kilo.ai/api/v1'
const OC_BASE = 'https://opencode.ai'
const CACHE_MS = 10 * 60 * 1000

const UPSTREAM_HEADERS = {
  'content-type': 'application/json',
  'accept': 'application/json',
  'http-referer': 'https://kilo.ai/',
  'x-title': 'dsh-our-free-model',
  'user-agent': 'dsh-our-free-model',
}

const OC_HEADERS = {
  'Authorization': 'Bearer public',
  'accept': 'application/json',
  'user-agent': 'opencode/2.1.0',
  'x-opencode-client': 'desktop',
}

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.json': 'application/json; charset=utf-8',
  '.ico': 'image/x-icon',
}

/* ---------- 基准快照（离线实测，非实时） ---------- */
let benchData = { measuredAt: '', results: [] }
try {
  benchData = JSON.parse(fs.readFileSync(BENCH_PATH, 'utf8'))
} catch {
  // 没有基准文件时功能照常，只是不显示速度数据
}
const benchMap = Object.fromEntries(
  (Array.isArray(benchData.results) ? benchData.results : []).map(x => [x.id, x])
)

function benchOf(id) {
  const b = benchMap[id]
  if (!b) return null
  // reasoning_only = 输出上限内只吐思考、没吐正文；模型本身可用，只是默认长思考
  const ok = b.status === 'ok' || b.status === 'reasoning_only'
  return {
    status: b.status,
    ttft: b.ttft ?? null,
    contentTtft: b.contentTtft ?? null,
    tps: b.tps ?? null,
    total: b.total ?? null,
    outChars: b.outChars ?? null,
    reasonChars: b.reasonChars ?? null,
    completionTokens: b.completionTokens ?? null,
    usable: ok,
    longThinking: b.status === 'reasoning_only' ||
      (b.contentTtft != null && b.ttft != null && b.contentTtft - b.ttft > 1.5),
    note: b.err || '',
  }
}

/* ---------- 通用响应 ---------- */
function cors(res) {
  res.setHeader('access-control-allow-origin', '*')
  res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS')
  res.setHeader('access-control-allow-headers',
    'authorization, content-type, accept, x-requested-with, http-referer, x-title, user-agent, cookie')
  res.setHeader('access-control-expose-headers', 'content-type, x-request-id')
  res.setHeader('access-control-max-age', '86400')
}
function sendJSON(res, code, obj) {
  res.writeHead(code, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  })
  res.end(JSON.stringify(obj))
}
function openAIError(res, code, message, type = 'invalid_request_error', extra = {}) {
  sendJSON(res, code, {
    error: {
      message: String(message).slice(0, 900),
      type,
      param: null,
      code: extra.code || null,
      ...(extra.upstream_status ? { upstream_status: extra.upstream_status } : {}),
      ...(extra.upstream_body ? { upstream_body: String(extra.upstream_body).slice(0, 700) } : {}),
    },
  })
}

function serveStatic(req, res, pathname) {
  if (pathname === '/' || pathname === '') pathname = '/index.html'
  const safe = path.normalize(pathname).replace(/^(\.\.[\/\\])+/, '')
  const file = path.join(PUBLIC_DIR, safe)
  if (!file.startsWith(PUBLIC_DIR)) {
    res.writeHead(403); res.end('Forbidden'); return
  }
  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' })
      res.end('Not Found')
      return
    }
    res.writeHead(200, {
      'content-type': MIME[path.extname(file)] || 'application/octet-stream',
      'cache-control': 'no-cache',
    })
    res.end(data)
  })
}

/**
 * 还原被拼接进 query 的真实路径。不同客户端对带 query 的 base_url 处理不同：
 *  - 标准 httpx / 字符串拼接：/v1?sharecode=XXX&/chat/completions
 *  - 新版 openai SDK(httpx2)： /v1/?sharecode=XXX&chat/completions
 * 两者都还原成 /v1/chat/completions。另把尾斜杠与重复斜杠规范化。
 */
const PATH_HINTS = ['chat/completions', 'models', 'completions']
function resolvePath(req) {
  const url = new URL(req.url, 'http://localhost')
  let p = url.pathname.replace(/\/{2,}/g, '/')
  for (const [k, v] of url.searchParams.entries()) {
    if (v !== '') continue
    const kk = k.replace(/^\/+/, '')
    const hit = PATH_HINTS.find(s => kk === s || kk.endsWith('/' + s))
    if (hit) {
      p = (p === '/' ? '' : p.replace(/\/+$/, '')) + '/' + hit
      break
    }
  }
  return { url, p: p.replace(/\/+$/, '') || '/' }
}

async function fetchJSON(url, opts = {}, timeout = 25000) {
  const r = await fetch(url, { ...opts, signal: AbortSignal.timeout(timeout) })
  const text = await r.text()
  let data = null
  try { data = JSON.parse(text) } catch { data = text }
  return { ok: r.ok, status: r.status, data }
}

const isFreeModel = (m) => m && typeof m.id === 'string' &&
  (m.isFree === true || String(m.pricing?.prompt) === '0')

let modelsCache = { at: 0, data: null }
let statusCache = { at: 0, data: null }

async function getFreeModels(force = false) {
  if (!force && modelsCache.data && Date.now() - modelsCache.at < CACHE_MS) return modelsCache.data
  const r = await fetchJSON(KILO_BASE + '/models', { headers: UPSTREAM_HEADERS })
  if (!r.ok || !Array.isArray(r.data?.data)) {
    throw new Error('上游模型清单不可用（HTTP ' + r.status + '）')
  }
  const total = r.data.data.length
  const list = r.data.data.filter(isFreeModel).map(m => {
    const sp = Array.isArray(m.supported_parameters) ? m.supported_parameters : []
    const inMods = Array.isArray(m.architecture?.input_modalities) ? m.architecture.input_modalities : []
    const outMods = Array.isArray(m.architecture?.output_modalities) ? m.architecture.output_modalities : []
    const b = benchOf(m.id)
    return {
      id: m.id,
      name: m.name || m.id,
      description: String(m.description || '').slice(0, 260),
      context: m.context_length || m.top_provider?.context_length || 0,
      maxOut: m.top_provider?.max_completion_tokens || 0,
      tokenizer: m.architecture?.tokenizer || '',
      priceIn: String(m.pricing?.prompt ?? '0'),
      priceOut: String(m.pricing?.completion ?? '0'),
      train: !!m.mayTrainOnYourPrompts,
      moderated: !!m.top_provider?.is_moderated,
      vision: inMods.includes('image'),
      video: inMods.includes('video'),
      audioIn: inMods.includes('audio'),
      audioOut: outMods.includes('audio'),
      imageOut: outMods.includes('image'),
      reasoning: sp.includes('reasoning') || sp.includes('include_reasoning'),
      tools: sp.includes('tools'),
      structured: sp.includes('structured_outputs') || sp.includes('response_format'),
      auto: m.id.startsWith('kilo-auto/') || m.id === 'openrouter/free',
      bench: b,
      usable: b ? b.usable : null,
      total,
    }
  })
  // 正常出正文的排最前，其次「默认长思考但可用」，再是不可用/未测
  const rank = (m) => {
    if (!m.bench) return 1
    if (m.bench.status === 'ok') return 4
    if (m.bench.status === 'reasoning_only') return 3
    if (m.usable === false) return 0
    return 2
  }
  list.sort((a, b) =>
    rank(b) - rank(a) ||
    (a.bench?.ttft ?? 999) - (b.bench?.ttft ?? 999) ||
    (b.auto ? 1 : 0) - (a.auto ? 1 : 0) ||
    a.name.localeCompare(b.name, 'zh'))
  modelsCache = { at: Date.now(), data: list }
  return list
}

/** 只读体检：两条车道都只拉模型清单，不发起推理、不消耗免费额度。 */
async function getLaneStatus(force = false) {
  if (!force && statusCache.data && Date.now() - statusCache.at < CACHE_MS) return statusCache.data
  const [kilo, oc] = await Promise.allSettled([
    fetchJSON(KILO_BASE + '/models', { headers: UPSTREAM_HEADERS }, 15000),
    fetchJSON(OC_BASE + '/inference/openai/v1/models', { headers: OC_HEADERS }, 15000),
  ])
  const out = {
    checkedAt: new Date().toISOString(),
    kilo: { ok: false, models: 0, note: '' },
    opencode: { ok: false, models: 0, note: '' },
    defaultLane: 'kilo',
    benchAt: benchData.measuredAt || '',
  }
  if (kilo.status === 'fulfilled') {
    const v = kilo.value
    out.kilo.ok = v.ok
    out.kilo.models = Array.isArray(v.data?.data) ? v.data.data.filter(isFreeModel).length : 0
    out.kilo.note = v.ok ? '模型清单可达，推理已实测可用' : 'HTTP ' + v.status
  } else out.kilo.note = '连接失败'
  if (oc.status === 'fulfilled') {
    const v = oc.value
    out.opencode.ok = v.ok
    out.opencode.models = Array.isArray(v.data?.data)
      ? v.data.data.filter(m => String(m.cost?.text?.input?.price) === '0' &&
          String(m.cost?.text?.output?.price) === '0').length
      : 0
    out.opencode.note = v.ok
      ? '清单可达，但免密推理按出口 IP 配额，实测返回 429 FreeUsageLimitError（额度耗尽）'
      : 'HTTP ' + v.status
  } else out.opencode.note = '连接失败'
  statusCache = { at: Date.now(), data: out }
  return out
}

function readBody(req, limit = 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    let tooLarge = false
    req.on('data', c => {
      size += c.length
      if (size > limit) tooLarge = true
      else if (!tooLarge) chunks.push(c)
    })
    req.on('end', () => {
      if (tooLarge) { reject(new Error('请求体过大')); return }
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')) }
      catch { reject(new Error('JSON 解析失败')) }
    })
    req.on('error', reject)
  })
}

/* ---------- 上游推理转发 ---------- */
const PASS_KEYS = [
  'max_tokens', 'max_completion_tokens', 'temperature', 'top_p', 'top_k', 'min_p',
  'stop', 'frequency_penalty', 'presence_penalty', 'repetition_penalty', 'seed',
  'response_format', 'tools', 'tool_choice', 'reasoning', 'reasoning_effort',
  'include_reasoning', 'stream_options', 'n', 'user', 'logprobs', 'logit_bias',
]

function normalizeMessages(raw) {
  if (!Array.isArray(raw)) return []
  return raw
    .filter(m => m && ['system', 'developer', 'user', 'assistant', 'tool'].includes(m.role))
    .map(m => {
      let content = m.content
      if (typeof content === 'string') content = content.slice(0, 100000)
      else if (content == null) content = ''
      else if (!Array.isArray(content)) content = String(content)
      const o = { role: m.role, content }
      if (typeof m.name === 'string') o.name = m.name.slice(0, 80)
      if (typeof m.tool_call_id === 'string') o.tool_call_id = m.tool_call_id.slice(0, 200)
      return o
    })
    .slice(-60)
}

async function callUpstream(body, stream, res) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 180000)
  let upstream
  try {
    upstream = await fetch(KILO_BASE + '/chat/completions', {
      method: 'POST',
      headers: {
        ...UPSTREAM_HEADERS,
        accept: stream ? 'text/event-stream' : 'application/json',
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    })
  } catch (e) {
    clearTimeout(timer)
    openAIError(res, 502, '上游连接失败：' + (e.message || e), 'upstream_error')
    return
  }
  if (!upstream.ok || !upstream.body) {
    clearTimeout(timer)
    const txt = await upstream.text().catch(() => '')
    let msg = '上游返回 HTTP ' + upstream.status
    try {
      const j = JSON.parse(txt)
      msg = j.error?.message || j.error?.code || j.error || msg
      if (typeof msg !== 'string') msg = String(msg)
    } catch { /* 保留原始状态 */ }
    openAIError(res, upstream.status, msg, 'upstream_error', {
      code: 'upstream_error',
      upstream_status: upstream.status,
      upstream_body: txt,
    })
    return
  }

  if (stream) {
    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      'connection': 'keep-alive',
      'x-accel-buffering': 'no',
    })
    const s = Readable.fromWeb(upstream.body)
    s.on('error', () => { try { res.end() } catch {} })
    s.on('end', () => clearTimeout(timer))
    res.on('close', () => {
      controller.abort(); clearTimeout(timer)
      try { s.destroy() } catch {}
    })
    s.pipe(res)
    return
  }

  clearTimeout(timer)
  const text = await upstream.text().catch(() => '')
  let j
  try { j = JSON.parse(text) }
  catch {
    openAIError(res, 502, '上游返回非 JSON 内容', 'upstream_error', { upstream_body: text })
    return
  }
  j.object = j.object || 'chat.completion'
  j.id = j.id || ('chatcmpl-' + Math.random().toString(36).slice(2, 14))
  j.created = j.created || Math.floor(Date.now() / 1000)
  j.model = j.model || body.model
  if (Array.isArray(j.choices)) {
    j.choices = j.choices.map((c, i) => ({
      ...c,
      index: c.index ?? i,
      finish_reason: c.finish_reason ?? 'stop',
    }))
  }
  sendJSON(res, 200, j)
}

/** 本站网页用：固定流式、收敛参数。 */
async function handleChat(req, res) {
  let payload
  try { payload = await readBody(req, 256 * 1024) }
  catch (e) { openAIError(res, 400, e.message); return }
  const model = typeof payload.model === 'string' && payload.model.trim()
    ? payload.model.trim().slice(0, 200) : 'kilo-auto/free'
  const messages = normalizeMessages(payload.messages)
    .filter(m => ['user', 'assistant', 'system'].includes(m.role) && typeof m.content === 'string')
    .slice(-30)
  if (!messages.length) { openAIError(res, 400, 'messages 不能为空'); return }
  const body = {
    model, messages, stream: true,
    max_tokens: Math.min(Math.max(Number(payload.max_tokens) || 2048, 64), 8192),
  }
  await callUpstream(body, true, res)
}

/** OpenAI 兼容：支持流式与非流式，透传常用采样参数。 */
async function handleV1Chat(req, res) {
  let payload
  try { payload = await readBody(req, 2 * 1024 * 1024) }
  catch (e) { openAIError(res, 400, e.message); return }
  const model = typeof payload.model === 'string' && payload.model.trim()
    ? payload.model.trim().slice(0, 200) : 'kilo-auto/free'
  const messages = normalizeMessages(payload.messages)
  if (!messages.length) { openAIError(res, 400, 'messages 不能为空'); return }
  const stream = payload.stream === true
  const body = { model, messages, stream }
  for (const k of PASS_KEYS) {
    if (payload[k] !== undefined) body[k] = payload[k]
  }
  if (body.max_tokens === undefined && body.max_completion_tokens === undefined) {
    body.max_tokens = 2048
  }
  await callUpstream(body, stream, res)
}

/** OpenAI 兼容：旧版 text completion，转成 chat 后补回 text 字段。 */
async function handleV1Completions(req, res) {
  let payload
  try { payload = await readBody(req, 2 * 1024 * 1024) }
  catch (e) { openAIError(res, 400, e.message); return }
  const prompt = typeof payload.prompt === 'string' ? payload.prompt
    : Array.isArray(payload.prompt) ? payload.prompt.join('\n') : ''
  if (!prompt.trim()) { openAIError(res, 400, 'prompt 不能为空'); return }
  if (payload.stream === true) {
    openAIError(res, 400, '/v1/completions 暂不支持流式，请使用 stream:false 或 /v1/chat/completions')
    return
  }
  const body = {
    model: typeof payload.model === 'string' && payload.model.trim() ? payload.model : 'kilo-auto/free',
    messages: [{ role: 'user', content: prompt }],
    stream: false,
    max_tokens: Number(payload.max_tokens) || 1024,
  }
  for (const k of ['temperature', 'top_p', 'stop', 'seed', 'n', 'frequency_penalty', 'presence_penalty']) {
    if (payload[k] !== undefined) body[k] = payload[k]
  }
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 180000)
  try {
    const up = await fetch(KILO_BASE + '/chat/completions', {
      method: 'POST',
      headers: UPSTREAM_HEADERS,
      body: JSON.stringify(body),
      signal: controller.signal,
    })
    clearTimeout(timer)
    const txt = await up.text()
    if (!up.ok) {
      openAIError(res, up.status, '上游返回 HTTP ' + up.status, 'upstream_error', { upstream_body: txt })
      return
    }
    const j = JSON.parse(txt)
    const choices = (j.choices || []).map((c, i) => ({
      text: c.message?.content ?? c.text ?? '',
      index: i,
      logprobs: null,
      finish_reason: c.finish_reason || 'stop',
    }))
    sendJSON(res, 200, {
      id: j.id || ('cmpl-' + Math.random().toString(36).slice(2, 14)),
      object: 'text_completion',
      created: j.created || Math.floor(Date.now() / 1000),
      model: j.model || body.model,
      choices,
      usage: j.usage || null,
    })
  } catch (e) {
    clearTimeout(timer)
    openAIError(res, 502, '上游连接失败：' + (e.message || e), 'upstream_error')
  }
}

async function v1Models(res) {
  const list = await getFreeModels()
  sendJSON(res, 200, {
    object: 'list',
    data: list.map(m => ({
      id: m.id,
      object: 'model',
      created: 0,
      owned_by: 'kilo-free-pool',
      name: m.name,
      context_length: m.context,
      max_completion_tokens: m.maxOut,
      pricing: { prompt: m.priceIn, completion: m.priceOut },
      input_modalities: [
        'text',
        ...(m.vision ? ['image'] : []),
        ...(m.video ? ['video'] : []),
        ...(m.audioIn ? ['audio'] : []),
      ],
      capabilities: {
        tools: m.tools, reasoning: m.reasoning, structured: m.structured,
        vision: m.vision, train_may_occur: m.train, moderated: m.moderated,
      },
      benchmark: m.bench ? { ttft: m.bench.ttft, tps: m.bench.tps, status: m.bench.status } : null,
    })),
  })
}

const server = http.createServer(async (req, res) => {
  cors(res)
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
  let url, p
  try { ({ url, p } = resolvePath(req)) }
  catch { res.writeHead(400); res.end('Bad Request'); return }

  try {
    /* ---- OpenAI 兼容 ---- */
    if (p === '/v1' || p === '/v1/' || p === '/v1/chat/completions' && req.method === 'GET') {
      if (req.method !== 'GET') { res.writeHead(405); res.end('Method Not Allowed'); return }
      sendJSON(res, 200, {
        object: 'service_info',
        service: 'Free Lane Chat — OpenAI 兼容网关',
        base_url_hint: '把当前地址的查询串保留在 base_url 末尾（以 & 结尾）',
        api_key: API_KEY,
        api_key_note: '上游免密，此 Key 仅为占位；填任意非空字符串都能用，不是访问控制',
        endpoints: {
          'GET /v1/models': '模型清单（含上下文、能力、实测性能）',
          'POST /v1/chat/completions': '对话补全，支持 stream:true / false',
          'POST /v1/completions': '旧版文本补全（仅非流式）',
        },
        upstream: 'Kilo AI 公共网关免费池',
        disclaimer: 'prompt 会发往第三方上游；免费额度随时可能限流或下线；请勿提交任何敏感数据',
        benchmark_at: benchData.measuredAt || null,
      })
      return
    }
    if ((p === '/v1/models' || p === '/v1/models/') && req.method === 'GET') {
      await v1Models(res); return
    }
    if (p.startsWith('/v1/models/') && req.method === 'GET') {
      const id = decodeURIComponent(p.slice('/v1/models/'.length))
      const list = await getFreeModels()
      const m = list.find(x => x.id === id)
      if (!m) { openAIError(res, 404, '模型不存在：' + id, 'invalid_request_error'); return }
      sendJSON(res, 200, {
        id: m.id, object: 'model', created: 0, owned_by: 'kilo-free-pool',
        name: m.name, context_length: m.context, max_completion_tokens: m.maxOut,
      })
      return
    }
    if (p === '/v1/chat/completions' && req.method === 'POST') {
      await handleV1Chat(req, res); return
    }
    if (p === '/v1/completions' && req.method === 'POST') {
      await handleV1Completions(req, res); return
    }
    if (p.startsWith('/v1/')) {
      openAIError(res, 404, '未知 OpenAI 兼容端点：' + p, 'invalid_request_error'); return
    }

    /* ---- 本站网页 ---- */
    if (p === '/api/models') {
      const list = await getFreeModels(url.searchParams.has('refresh'))
      sendJSON(res, 200, {
        models: list,
        total: list[0]?.total ?? list.length,
        benchmarkAt: benchData.measuredAt || '',
        apiKey: API_KEY,
      })
      return
    }
    if (p === '/api/status') {
      sendJSON(res, 200, await getLaneStatus(url.searchParams.has('refresh')))
      return
    }
    if (p === '/api/bench') {
      sendJSON(res, 200, benchData)
      return
    }
    if (p === '/api/chat' && req.method === 'POST') {
      await handleChat(req, res); return
    }
    if (p.startsWith('/api/')) { openAIError(res, 404, '未知接口'); return }
    if (req.method === 'GET' || req.method === 'HEAD') { serveStatic(req, res, p); return }
    res.writeHead(405); res.end('Method Not Allowed')
  } catch (e) {
    if (!res.headersSent) openAIError(res, 500, String(e?.message || e), 'server_error')
    else { try { res.end() } catch {} }
  }
})

server.listen(PORT, HOST, () => {
  console.log('Free Lane Chat listening on ' + HOST + ':' + PORT + '  (OpenAI 兼容: /v1)')
})
