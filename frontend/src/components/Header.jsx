import { Plus } from 'lucide-react';
import vantyraIcon from '../assets/vantyra-icon.png';

export default function Header({ onNewChat }) {
  return (
    <header className="flex items-center h-20 px-4 bg-vantyra-bg-s border-b border-vantyra-border shrink-0">
      <img src={vantyraIcon} alt="Vantyra" className="h-14 w-14 object-contain" />
      <span className="ml-2 text-2xl font-semibold tracking-tight">
        <span className="text-white">Vantyra</span>{' '}
        <span className="text-vantyra-accent">Analytics</span>
      </span>

      <div className="flex-1" />

      <button
        onClick={onNewChat}
        className="flex items-center gap-1.5 rounded-full bg-vantyra-accent px-4 py-1.5 text-sm font-medium text-vantyra-bg hover:brightness-110 transition"
      >
        <Plus size={16} />
        New Chat
      </button>
    </header>
  );
}
