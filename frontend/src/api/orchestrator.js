import { CONFIG } from '../config.js';
import { inviteHeaders } from '../utils/inviteCode.js';
import { userIdHeaders } from '../utils/userIdentity.js';

const BASE = CONFIG.orchestratorUrl;

/** Merge all custom headers for orchestrator requests. */
function authHeaders() {
  return { ...inviteHeaders(), ...userIdHeaders() };
}

/**
 * POST /chat — Send a message to the orchestrator
 * @param {string|null} conversationId — null for the first message in a new conversation
 * @param {string} message
 * @param {string|null} [fileContext] — file metadata/contents sent as separate context
 * @returns {Promise<{conversation_id: string, response_type: string, content: string, status: string}>}
 */
export async function sendMessage(conversationId, message, fileContext = null) {
  const payload = { conversation_id: conversationId, message };
  if (fileContext) payload.file_context = fileContext;
  const res = await fetch(`${BASE}/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`Chat failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * POST /chat/approve — Approve and execute a pending plan
 * @param {string} conversationId
 * @returns {Promise<{conversation_id: string, content: string, success: boolean, steps: Array, status: string}>}
 */
export async function approvePlan(conversationId) {
  const res = await fetch(`${BASE}/chat/approve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify({ conversation_id: conversationId }),
  });
  if (!res.ok) throw new Error(`Approve failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * GET /conversations — List conversations for the current user
 * @returns {Promise<Array<{id: string, title: string|null, updated_at: string}>>}
 */
export async function getConversations() {
  const res = await fetch(`${BASE}/conversations`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Conversations list failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * GET /conversations/{conversationId} — Get conversation with messages
 * @param {string} conversationId
 * @returns {Promise<{id: string, title: string|null, created_at: string, updated_at: string, messages: Array}>}
 */
export async function getConversation(conversationId) {
  const res = await fetch(`${BASE}/conversations/${conversationId}`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Conversation fetch failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * POST /conversations/{conversationId}/title — Rename a conversation
 * @param {string} conversationId
 * @param {string} title
 * @returns {Promise<{status: string}>}
 */
export async function renameConversation(conversationId, title) {
  const res = await fetch(`${BASE}/conversations/${conversationId}/title`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify({ title }),
  });
  if (!res.ok) throw new Error(`Rename failed: ${res.status} ${res.statusText}`);
  return res.json();
}

/**
 * DELETE /conversations/{conversationId} — Delete a conversation
 * @param {string} conversationId
 * @returns {Promise<boolean>} true if deleted successfully
 */
export async function deleteConversation(conversationId) {
  const res = await fetch(`${BASE}/conversations/${conversationId}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  return res.status === 204;
}

/**
 * GET /health — Check orchestrator health
 * @returns {Promise<object>}
 */
export async function checkHealth() {
  const res = await fetch(`${BASE}/health`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Health check failed: ${res.status}`);
  return res.json();
}
