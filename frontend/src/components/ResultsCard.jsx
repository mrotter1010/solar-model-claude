import ReactMarkdown from 'react-markdown';
import { Download } from 'lucide-react';
import { CONFIG } from '../config.js';
import StepsList from './StepsList';

const RUN_TOOLS = ['run_production', 'run_bill_savings', 'run_bess', 'run_buildability'];
const TOOLS_WITH_TIMESERIES = ['run_production', 'run_bill_savings', 'run_bess'];

function formatToolName(name) {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

function buildDownloadUrl(baseUrl, runId, type) {
  const suffix = type === 'pdf' ? 'report' : 'timeseries';
  const url = `${baseUrl}/analyses/${runId}/${suffix}`;
  if (CONFIG.analysisApiKey) {
    return `${url}?api_key=${CONFIG.analysisApiKey}`;
  }
  return url;
}

export default function ResultsCard({ content, steps, analysisApiUrl }) {
  const downloadableSteps = steps.filter(s =>
    s.success && RUN_TOOLS.includes(s.tool) && s.result?.run_id
  );

  return (
    <div className="flex justify-start">
      <div className="max-w-[75%] rounded-lg border border-green-300 bg-green-50 px-4 py-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-green-700 mb-2">
          Analysis Results
        </p>
        <div className="prose prose-sm max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-pre:bg-gray-800 prose-pre:text-gray-100 text-gray-900">
          <ReactMarkdown>{content}</ReactMarkdown>
        </div>
        {downloadableSteps.length > 0 && (
          <div className="mt-3 border-t border-green-200 pt-3 space-y-2">
            {downloadableSteps.map((step, idx) => (
              <div key={idx} className="text-sm">
                <p className="text-xs font-medium text-gray-600 mb-1">
                  {formatToolName(step.tool)}
                </p>
                <div className="flex gap-2">
                  <a
                    href={buildDownloadUrl(analysisApiUrl, step.result.run_id, 'pdf')}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 rounded bg-green-600 px-3 py-1 text-xs font-medium text-white hover:bg-green-700"
                  >
                    <Download size={12} />
                    Download PDF
                  </a>
                  {TOOLS_WITH_TIMESERIES.includes(step.tool) && (
                    <a
                      href={buildDownloadUrl(analysisApiUrl, step.result.run_id, 'csv')}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 rounded bg-gray-600 px-3 py-1 text-xs font-medium text-white hover:bg-gray-700"
                    >
                      <Download size={12} />
                      Download CSV
                    </a>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
        <StepsList steps={steps} />
      </div>
    </div>
  );
}
