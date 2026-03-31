import ReactMarkdown from 'react-markdown';
import { CheckCircle } from 'lucide-react';

export default function PlanCard({ content, onApprove, isLoading }) {
  return (
    <div className="flex justify-start">
      <div className="max-w-[75%] rounded-lg border border-amber-300 bg-amber-50 px-4 py-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-amber-700 mb-2">
          Proposed Plan
        </p>
        <div className="prose prose-sm max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-pre:bg-gray-800 prose-pre:text-gray-100 text-gray-900">
          <ReactMarkdown>{content}</ReactMarkdown>
        </div>
        {onApprove && (
          <div className="mt-3 border-t border-amber-200 pt-3">
            <button
              onClick={onApprove}
              disabled={isLoading}
              className="flex items-center gap-2 rounded-lg bg-green-600 px-4 py-2 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <CheckCircle size={16} />
              {isLoading ? 'Executing...' : 'Approve & Execute'}
            </button>
            <p className="mt-2 text-xs text-gray-500">
              Or type a message to refine the plan
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
