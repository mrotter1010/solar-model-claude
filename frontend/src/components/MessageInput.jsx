import { useState, useRef, useEffect, useCallback } from 'react';
import { Send, X, Paperclip } from 'lucide-react';

const MAX_HEIGHT = 200;

export default function MessageInput({ onSend, disabled, leftSlot, pendingUpload, onClearPendingUpload }) {
  const [text, setText] = useState('');
  const textareaRef = useRef(null);

  const resizeTextarea = useCallback(() => {
    const ta = textareaRef.current;
    if (!ta) return;
    ta.style.height = 'auto';
    const clamped = Math.min(ta.scrollHeight, MAX_HEIGHT);
    ta.style.height = clamped + 'px';
    ta.style.overflowY = ta.scrollHeight > MAX_HEIGHT ? 'auto' : 'hidden';
  }, []);

  useEffect(() => {
    textareaRef.current?.focus();
  }, []);

  // Resize whenever text changes (typing, paste, or clear after send)
  useEffect(() => {
    resizeTextarea();
  }, [text, resizeTextarea]);

  const canSend = !disabled && (text.trim() || pendingUpload);

  const handleSend = () => {
    if (!canSend) return;
    onSend(text.trim());
    setText('');
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="border-t border-vantyra-border bg-vantyra-bg-s p-4">
      <div className="max-w-4xl mx-auto">
        {/* Pending file chip */}
        {pendingUpload && (
          <div className="mb-2 flex items-center">
            <span className="inline-flex items-center gap-1.5 rounded-md bg-vantyra-bg-h border border-vantyra-border px-2.5 py-1 text-xs text-vantyra-text">
              <Paperclip size={12} className="text-vantyra-text-s" />
              <span className="max-w-[200px] truncate">{pendingUpload.filename}</span>
              <button
                type="button"
                onClick={onClearPendingUpload}
                className="ml-0.5 rounded hover:bg-vantyra-bg-s hover:text-vantyra-accent"
                title="Remove attachment"
              >
                <X size={12} />
              </button>
            </span>
          </div>
        )}

        <div className="flex items-end gap-2">
          {leftSlot}
          <textarea
            ref={textareaRef}
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={disabled}
            placeholder={pendingUpload ? "Add a message or send the file..." : "Type a message..."}
            rows={1}
            className="flex-1 resize-none rounded-lg border border-vantyra-border bg-vantyra-bg-s px-3 py-2 text-sm text-vantyra-text placeholder:text-vantyra-text-s caret-vantyra-accent focus:outline-none focus:ring-2 focus:ring-vantyra-accent focus:border-transparent disabled:opacity-50 disabled:bg-vantyra-bg transition-[height] duration-150 ease-out"
            style={{ overflowY: 'hidden', backgroundColor: '#252540', color: '#e8e8f0', caretColor: '#00D4FF' }}
          />
          <button
            onClick={handleSend}
            disabled={!canSend}
            className="rounded-lg bg-vantyra-accent p-2 text-vantyra-bg hover:brightness-110 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <Send size={18} />
          </button>
        </div>
      </div>
    </div>
  );
}
