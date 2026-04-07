import { useState, useRef, useEffect, useCallback } from 'react';
import { ChevronLeft, Menu, MoreVertical } from 'lucide-react';

function timeAgo(timestamp) {
  const seconds = Math.floor((Date.now() - timestamp) / 1000);
  if (seconds < 60) return 'just now';
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export default function Sidebar({
  isOpen,
  onToggle,
  conversations = [],
  activeConversationId,
  onSelectConversation,
  onDeleteConversation,
  onRenameConversation,
}) {
  const [menuId, setMenuId] = useState(null);
  const [renamingId, setRenamingId] = useState(null);
  const [renameValue, setRenameValue] = useState('');
  const [deletingId, setDeletingId] = useState(null);
  const renameInputRef = useRef(null);

  // Focus and select the rename input when entering rename mode
  useEffect(() => {
    if (renamingId) {
      renameInputRef.current?.focus();
      renameInputRef.current?.select();
    }
  }, [renamingId]);

  // Close all interactive states when sidebar closes
  useEffect(() => {
    if (!isOpen) {
      setMenuId(null);
      setRenamingId(null);
      setDeletingId(null);
    }
  }, [isOpen]);

  const handleMenuToggle = useCallback((id, e) => {
    e.stopPropagation();
    setMenuId(prev => (prev === id ? null : id));
    setDeletingId(null);
    setRenamingId(null);
  }, []);

  const handleRenameStart = useCallback((conv, e) => {
    e.stopPropagation();
    setMenuId(null);
    setDeletingId(null);
    setRenamingId(conv.id);
    setRenameValue(conv.title || '');
  }, []);

  const handleRenameSubmit = useCallback(async () => {
    const trimmed = renameValue.trim();
    if (!trimmed || !renamingId) {
      setRenamingId(null);
      return;
    }
    try {
      await onRenameConversation?.(renamingId, trimmed);
    } catch (err) {
      console.error('Failed to rename conversation:', err);
    }
    setRenamingId(null);
  }, [renamingId, renameValue, onRenameConversation]);

  const handleRenameCancel = useCallback(() => {
    setRenamingId(null);
  }, []);

  const handleRenameKeyDown = useCallback(
    (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        handleRenameSubmit();
      } else if (e.key === 'Escape') {
        e.preventDefault();
        handleRenameCancel();
      }
    },
    [handleRenameSubmit, handleRenameCancel],
  );

  const handleDeleteStart = useCallback((id, e) => {
    e.stopPropagation();
    setMenuId(null);
    setRenamingId(null);
    setDeletingId(id);
  }, []);

  const handleDeleteConfirm = useCallback(
    async (id, e) => {
      e.stopPropagation();
      setDeletingId(null);
      try {
        await onDeleteConversation?.(id);
      } catch (err) {
        console.error('Failed to delete conversation:', err);
      }
    },
    [onDeleteConversation],
  );

  const handleDeleteCancel = useCallback((e) => {
    e.stopPropagation();
    setDeletingId(null);
  }, []);

  return (
    <>
      {/* Mobile overlay backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/40 z-20 lg:hidden"
          onClick={onToggle}
        />
      )}

      <aside
        className={`
          shrink-0 flex flex-col bg-vantyra-bg-s border-r border-vantyra-border
          transition-[width] duration-200 ease-in-out overflow-hidden z-30
          ${isOpen ? 'w-[280px]' : 'w-0'}
          fixed lg:relative h-full
        `}
      >
        {/* Sidebar header — toggle + label, h-20 to align with Header */}
        <div className="flex items-center gap-2 h-20 px-3 border-b border-vantyra-border shrink-0">
          <button
            onClick={onToggle}
            className="p-1.5 rounded-lg text-vantyra-text-s hover:bg-vantyra-bg-h hover:text-vantyra-text transition"
            title="Close sidebar"
          >
            <ChevronLeft size={20} />
          </button>
          <span className="text-lg font-medium text-vantyra-text">
            Message History
          </span>
        </div>

        {/* Conversation list */}
        <div className="flex-1 overflow-y-auto p-2 space-y-0.5">
          {conversations.length === 0 ? (
            <p className="text-xs text-vantyra-text-s text-center mt-8 px-4">
              No conversations yet
            </p>
          ) : (
            conversations.map((conv) => {
              const isActive = conv.id === activeConversationId;
              const isMenuOpen = menuId === conv.id;
              const isRenaming = renamingId === conv.id;
              const isDeleting = deletingId === conv.id;

              return (
                <div key={conv.id} className="relative">
                  {/* Invisible overlay to catch click-outside for menu */}
                  {isMenuOpen && (
                    <div
                      className="fixed inset-0 z-10"
                      onClick={() => setMenuId(null)}
                    />
                  )}

                  <button
                    onClick={() => {
                      if (!isRenaming && !isDeleting) {
                        onSelectConversation?.(conv.id);
                      }
                    }}
                    className={`w-full group flex items-center gap-2 rounded-lg px-3 py-2.5 text-left text-sm transition ${
                      isActive
                        ? 'bg-vantyra-accent/10 border-l-2 border-vantyra-accent text-vantyra-text'
                        : 'border-l-2 border-transparent text-gray-200 hover:bg-vantyra-bg-h hover:text-vantyra-text'
                    }`}
                  >
                    <div className="flex-1 min-w-0">
                      {isRenaming ? (
                        <input
                          ref={renameInputRef}
                          type="text"
                          value={renameValue}
                          onChange={(e) => setRenameValue(e.target.value)}
                          onKeyDown={handleRenameKeyDown}
                          onBlur={handleRenameSubmit}
                          onClick={(e) => e.stopPropagation()}
                          className="w-full bg-vantyra-bg border border-vantyra-border rounded px-2 py-0.5 text-sm text-vantyra-text focus:outline-none focus:border-vantyra-accent"
                        />
                      ) : isDeleting ? (
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-vantyra-text-s shrink-0">
                            Delete?
                          </span>
                          <button
                            onClick={handleDeleteCancel}
                            className="text-xs text-vantyra-text-s hover:text-vantyra-text transition"
                          >
                            Cancel
                          </button>
                          <button
                            onClick={(e) => handleDeleteConfirm(conv.id, e)}
                            className="text-xs text-vantyra-error hover:text-red-400 font-medium transition"
                          >
                            Delete
                          </button>
                        </div>
                      ) : (
                        <>
                          <p className="truncate">
                            {conv.title || 'Untitled'}
                          </p>
                          <p className="text-xs text-gray-400 mt-0.5">
                            {timeAgo(new Date(conv.updated_at).getTime())}
                          </p>
                        </>
                      )}
                    </div>

                    {/* Three-dot menu trigger — hidden during rename/delete */}
                    {!isRenaming && !isDeleting && (
                      <span
                        role="button"
                        tabIndex={0}
                        onClick={(e) => handleMenuToggle(conv.id, e)}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') handleMenuToggle(conv.id, e);
                        }}
                        className={`shrink-0 p-1 rounded hover:bg-vantyra-bg text-vantyra-text-s hover:text-vantyra-text transition ${
                          isMenuOpen
                            ? 'opacity-100'
                            : 'opacity-0 group-hover:opacity-100'
                        }`}
                      >
                        <MoreVertical size={14} />
                      </span>
                    )}
                  </button>

                  {/* Context menu dropdown */}
                  {isMenuOpen && (
                    <div className="absolute right-2 top-full mt-1 z-20 bg-vantyra-bg-s border border-vantyra-border rounded-lg shadow-lg py-1 min-w-[120px]">
                      <button
                        onClick={(e) => handleRenameStart(conv, e)}
                        className="w-full text-left px-3 py-1.5 text-sm text-vantyra-text hover:bg-vantyra-bg-h transition"
                      >
                        Rename
                      </button>
                      <button
                        onClick={(e) => handleDeleteStart(conv.id, e)}
                        className="w-full text-left px-3 py-1.5 text-sm text-vantyra-error hover:bg-vantyra-bg-h transition"
                      >
                        Delete
                      </button>
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      </aside>

      {/* Toggle button — always visible, outside the collapsible area */}
      {!isOpen && (
        <button
          onClick={onToggle}
          className="fixed top-[26px] left-3 z-30 p-1.5 rounded-lg bg-vantyra-bg-s border border-vantyra-border text-vantyra-text-s hover:bg-vantyra-bg-h hover:text-vantyra-text transition"
          title="Open sidebar"
        >
          <Menu size={20} />
        </button>
      )}
    </>
  );
}
