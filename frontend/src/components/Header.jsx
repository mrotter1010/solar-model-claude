import { Plus, Download } from 'lucide-react';
import vantyraIcon from '../assets/vantyra-icon.png';
import { CONFIG } from '../config.js';

function buildTemplateUrl() {
  const url = `${CONFIG.analysisApiUrl}/analyses/batch/template?format=xlsx`;
  if (CONFIG.analysisApiKey) {
    return `${url}&api_key=${CONFIG.analysisApiKey}`;
  }
  return url;
}

export default function Header({ onNewChat }) {
  return (
    <header className="flex items-center h-20 pl-12 pr-4 bg-vantyra-bg-s border-b border-vantyra-border shrink-0">
      <img src={vantyraIcon} alt="Vantyra" className="h-14 w-14 object-contain" />
      <span className="ml-2 text-2xl font-semibold tracking-tight">
        <span className="text-white">Vantyra</span>{' '}
        <span className="text-vantyra-accent">Analytics</span>
      </span>

      <div className="flex-1" />

      <a
        href={buildTemplateUrl()}
        target="_blank"
        rel="noopener noreferrer"
        className="flex items-center gap-1.5 rounded-full border border-vantyra-border px-3 py-1.5 text-xs font-medium text-vantyra-text-s hover:text-vantyra-text hover:border-vantyra-accent transition mr-3"
      >
        <Download size={14} />
        Batch Template
      </a>

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
