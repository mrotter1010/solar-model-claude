import { CONFIG } from '../config.js';
import { inviteHeaders } from '../utils/inviteCode.js';

const BASE = CONFIG.orchestratorUrl;

/**
 * Parse a single SSE event block into {type, data}.
 * @param {string} raw — one SSE event (lines separated by \n)
 * @returns {{type: string, data: object}|null}
 */
function parseSSEEvent(raw) {
  let eventType = null;
  let dataStr = null;

  for (const line of raw.split('\n')) {
    if (line.startsWith('event:')) {
      eventType = line.slice(6).trim();
    } else if (line.startsWith('data:')) {
      dataStr = line.slice(5).trim();
    }
  }

  if (!eventType || !dataStr) return null;

  try {
    return { type: eventType, data: JSON.parse(dataStr) };
  } catch {
    return null;
  }
}

/**
 * Stream plan approval via SSE (POST /chat/approve/stream).
 *
 * Uses fetch + ReadableStream since EventSource only supports GET.
 *
 * @param {string} sessionId
 * @param {(update: object) => void} onStepUpdate — called with step data for
 *   real-time UI updates. Each call includes {step_number, tool_name, status}
 *   where status is 'running' or 'complete'.
 * @returns {{promise: Promise<{synthesis: string|null, completedSteps: Array, error: string|null}>, cancel: () => void}}
 */
export function streamApproval(sessionId, onStepUpdate) {
  const controller = new AbortController();

  const promise = (async () => {
    const res = await fetch(`${BASE}/chat/approve/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...inviteHeaders() },
      body: JSON.stringify({ session_id: sessionId }),
      signal: controller.signal,
    });

    if (!res.ok) {
      throw new Error(`Stream failed: ${res.status} ${res.statusText}`);
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let synthesis = null;
    let error = null;
    const completedSteps = [];

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // SSE events are separated by \n\n
      const parts = buffer.split('\n\n');
      buffer = parts.pop(); // keep incomplete tail

      for (const part of parts) {
        if (!part.trim()) continue;
        const event = parseSSEEvent(part);
        if (!event) continue;

        switch (event.type) {
          case 'step_start':
            onStepUpdate?.({
              step_number: event.data.step_number,
              tool_name: event.data.tool_name,
              status: 'running',
              success: null,
              result_summary: null,
            });
            break;
          case 'step_complete':
            completedSteps.push(event.data);
            onStepUpdate?.({
              step_number: event.data.step_number,
              tool_name: event.data.tool_name,
              status: 'complete',
              success: event.data.success,
              result_summary: event.data.result_summary,
            });
            break;
          case 'synthesis':
            synthesis = event.data.text;
            break;
          case 'error':
            error = event.data.message;
            break;
          case 'done':
            // Terminal event — loop will end on next read
            break;
        }
      }
    }

    // Process remaining buffer
    if (buffer.trim()) {
      const event = parseSSEEvent(buffer);
      if (event?.type === 'synthesis') synthesis = event.data.text;
      if (event?.type === 'error') error = event.data.message;
    }

    return { synthesis, completedSteps, error };
  })();

  return { promise, cancel: () => controller.abort() };
}
