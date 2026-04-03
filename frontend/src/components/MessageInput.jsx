import { useState, useRef, useEffect, useCallback } from 'react';
import { Send } from 'lucide-react';

const MAX_HEIGHT = 200;

export default function MessageInput({ onSend, disabled, leftSlot }) {
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

  const handleSend = () => {
    const trimmed = text.trim();
    if (!trimmed || disabled) return;
    onSend(trimmed);
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
      <div className="flex items-end gap-2 max-w-4xl mx-auto">
        {leftSlot}
        <textarea
          ref={textareaRef}
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={disabled}
          placeholder="Type a message..."
          rows={1}
          className="flex-1 resize-none rounded-lg border border-vantyra-border bg-vantyra-bg-s px-3 py-2 text-sm text-vantyra-text placeholder:text-vantyra-text-s caret-vantyra-accent focus:outline-none focus:ring-2 focus:ring-vantyra-accent focus:border-transparent disabled:opacity-50 disabled:bg-vantyra-bg transition-[height] duration-150 ease-out"
          style={{ overflowY: 'hidden', backgroundColor: '#252540', color: '#e8e8f0', caretColor: '#00D4FF' }}
        />
        <button
          onClick={handleSend}
          disabled={disabled || !text.trim()}
          className="rounded-lg bg-vantyra-accent p-2 text-vantyra-bg hover:brightness-110 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <Send size={18} />
        </button>
      </div>
    </div>
  );
}
