import { CONFIG } from '../config.js';
import { inviteHeaders } from '../utils/inviteCode.js';

const BASE = CONFIG.orchestratorUrl;

/**
 * POST /chat — Send a message to the orchestrator
 * @param {string} sessionId
 * @param {string} message
 * @param {string|null} [fileContext] — file metadata/contents sent as separate context
 * @returns {Promise<{session_id: string, response_type: string, content: string, status: string}>}
 */
export async function sendMessage(sessionId, message, fileContext = null) {
  const payload = { session_id: sessionId, message };
  if (fileContext) payload.file_context = fileContext;
  const res = await fetch(`${BASE}/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...inviteHeaders() },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`Chat failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * POST /chat/approve — Approve and execute a pending plan
 * @param {string} sessionId
 * @returns {Promise<{session_id: string, content: string, success: boolean, steps: Array, status: string}>}
 */
export async function approvePlan(sessionId) {
  const res = await fetch(`${BASE}/chat/approve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...inviteHeaders() },
    body: JSON.stringify({ session_id: sessionId }),
  });
  if (!res.ok) throw new Error(`Approve failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * GET /sessions/{sessionId} — Get session info
 * @param {string} sessionId
 * @returns {Promise<{session_id: string, status: string, message_count: number, run_ids: string[], created_at: string, last_activity: string}>}
 */
export async function getSession(sessionId) {
  const res = await fetch(`${BASE}/sessions/${sessionId}`, {
    headers: inviteHeaders(),
  });
  if (!res.ok) throw new Error(`Session fetch failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * GET /health — Check orchestrator health
 * @returns {Promise<object>}
 */
export async function checkHealth() {
  const res = await fetch(`${BASE}/health`, {
    headers: inviteHeaders(),
  });
  if (!res.ok) throw new Error(`Health check failed: ${res.status}`);
  return res.json();
}
