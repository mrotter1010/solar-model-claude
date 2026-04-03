import ReactMarkdown from 'react-markdown';
import { CheckCircle } from 'lucide-react';

export default function PlanCard({ content, onApprove, isLoading }) {
  return (
    <div className="flex justify-start">
      <div className="max-w-[75%] rounded-lg border border-vantyra-border bg-vantyra-bg-s px-4 py-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-vantyra-accent mb-2">
          Proposed Plan
        </p>
        <div className="prose prose-sm prose-invert max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-pre:bg-[#1a1a2e] prose-pre:text-gray-100 prose-a:text-vantyra-accent prose-headings:text-vantyra-text prose-strong:text-vantyra-text prose-code:text-vantyra-accent">
          <ReactMarkdown>{content}</ReactMarkdown>
        </div>
        {onApprove && (
          <div className="mt-3 border-t border-vantyra-border pt-3">
            <button
              onClick={onApprove}
              disabled={isLoading}
              className="flex items-center gap-2 rounded-xl border-2 border-cyan-300 bg-cyan-500 px-8 py-3 text-sm font-bold text-white shadow-[0_0_24px_rgba(0,212,255,0.5)] hover:scale-105 hover:brightness-110 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <CheckCircle size={16} />
              {isLoading ? 'Executing...' : 'Approve & Execute'}
            </button>
            <p className="mt-2 text-xs text-vantyra-text-s">
              Or type a message to refine the plan
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
