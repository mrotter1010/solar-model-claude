import { useState, useCallback } from 'react';
import { sendMessage as apiSendMessage, approvePlan as apiApprovePlan } from '../api/orchestrator.js';
import { streamApproval } from '../api/sseClient.js';

function getOrCreateSessionId() {
  let id = sessionStorage.getItem('sessionId');
  if (!id) {
    id = crypto.randomUUID();
    sessionStorage.setItem('sessionId', id);
  }
  return id;
}

/**
 * Build a steps array compatible with ResultsCard from streaming step data.
 * Uses the full `result` object included in each step_complete SSE event
 * (promoted from step_data by the backend serializer) so that run_id and
 * other fields are available for download links.
 */
function buildStepsFromStream(completedSteps) {
  return completedSteps.map(s => ({
    tool: s.tool_name,
    arguments: {},
    result: s.result ?? null,
    success: s.success,
    error: !s.success ? (s.result?.error || s.result_summary) : null,
  }));
}

export function useChat() {
  const [sessionId] = useState(getOrCreateSessionId);
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [pendingPlan, setPendingPlan] = useState(false);
  const [executionSteps, setExecutionSteps] = useState([]);

  const sendMessage = useCallback(async (text) => {
    const userMsg = {
      id: crypto.randomUUID(),
      role: 'user',
      content: text,
      responseType: null,
      steps: null,
      timestamp: Date.now(),
    };
    setMessages(prev => [...prev, userMsg]);
    setIsLoading(true);

    try {
      const res = await apiSendMessage(sessionId, text);
      const assistantMsg = {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: res.content,
        responseType: res.response_type,
        steps: null,
        timestamp: Date.now(),
      };
      setMessages(prev => [...prev, assistantMsg]);
      setPendingPlan(res.response_type === 'plan');
    } catch (err) {
      const errorMsg = {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: err.message,
        responseType: 'error',
        steps: null,
        timestamp: Date.now(),
      };
      setMessages(prev => [...prev, errorMsg]);
    } finally {
      setIsLoading(false);
    }
  }, [sessionId]);

  const approvePlan = useCallback(async () => {
    setIsLoading(true);
    setPendingPlan(false);
    setExecutionSteps([]);

    // --- Try streaming path first ---
    try {
      const { promise } = streamApproval(sessionId, (stepUpdate) => {
        setExecutionSteps(prev => {
          const existing = prev.find(s => s.step_number === stepUpdate.step_number);
          if (existing) {
            return prev.map(s =>
              s.step_number === stepUpdate.step_number ? stepUpdate : s
            );
          }
          return [...prev, stepUpdate];
        });
      });

      const { synthesis, completedSteps, error } = await promise;

      // Stream completed — build final message
      const steps = buildStepsFromStream(completedSteps);
      const content = synthesis || error || 'Execution completed.';

      const assistantMsg = {
        id: crypto.randomUUID(),
        role: 'assistant',
        content,
        responseType: error && !synthesis ? 'error' : 'response',
        steps: steps.length > 0 ? steps : null,
        timestamp: Date.now(),
      };
      setMessages(prev => [...prev, assistantMsg]);
      setExecutionSteps([]);
      setIsLoading(false);
      return;
    } catch (err) {
      // Stream connection failed — fall back to sync
      console.info('SSE streaming failed, falling back to sync /chat/approve:', err.message);
      setExecutionSteps([]);
    }

    // --- Fallback: sync path (identical to original behavior) ---
    try {
      const res = await apiApprovePlan(sessionId);
      const assistantMsg = {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: res.content,
        responseType: 'response',
        steps: res.steps,
        timestamp: Date.now(),
      };
      setMessages(prev => [...prev, assistantMsg]);
    } catch (err) {
      const errorMsg = {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: err.message,
        responseType: 'error',
        steps: null,
        timestamp: Date.now(),
      };
      setMessages(prev => [...prev, errorMsg]);
    } finally {
      setIsLoading(false);
    }
  }, [sessionId]);

  const injectUploadMessage = useCallback((filename, serverPath, fileType) => {
    const text = `[System: File uploaded — ${fileType} file '${filename}' available at server path: ${serverPath}]`;
    sendMessage(text);
  }, [sendMessage]);

  return { sessionId, messages, isLoading, pendingPlan, executionSteps, sendMessage, approvePlan, injectUploadMessage };
}
