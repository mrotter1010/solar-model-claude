import { useState, useCallback } from 'react';
import { sendMessage as apiSendMessage, approvePlan as apiApprovePlan } from '../api/orchestrator.js';

function getOrCreateSessionId() {
  let id = sessionStorage.getItem('sessionId');
  if (!id) {
    id = crypto.randomUUID();
    sessionStorage.setItem('sessionId', id);
  }
  return id;
}

export function useChat() {
  const [sessionId] = useState(getOrCreateSessionId);
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [pendingPlan, setPendingPlan] = useState(false);

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

  return { sessionId, messages, isLoading, pendingPlan, sendMessage, approvePlan, injectUploadMessage };
}
