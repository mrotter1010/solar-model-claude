import { useState } from 'react';

const MAP_PATTERNS = ['land_cover_map', 'slope_map', 'buildability_map'];

export function ChatImage({ src, alt }) {
  const [status, setStatus] = useState('loading');
  const isMap = MAP_PATTERNS.some(p => src?.includes(p));

  if (status === 'error') {
    return (
      <div className="my-2 rounded-md border border-gray-700 bg-gray-800/50 px-4 py-3 text-sm text-gray-400">
        Failed to load image{alt ? `: ${alt}` : ''}
      </div>
    );
  }

  return (
    <div className="my-2">
      {status === 'loading' && (
        <div className="h-48 animate-pulse rounded-md bg-gray-800/50" />
      )}
      <img
        src={src}
        alt={alt || 'Chart'}
        onLoad={() => setStatus('loaded')}
        onError={() => setStatus('error')}
        className={`${isMap ? 'max-w-[560px] w-auto' : 'max-w-full'} rounded-md border border-gray-700 ${
          status === 'loading' ? 'hidden' : ''
        }`}
      />
    </div>
  );
}

export const markdownComponents = {
  p: ({ node, children, ...props }) => {
    // Check if this paragraph contains only an image (handles raw HTML <img> inside <p>)
    const child = node?.children?.[0];
    if (node?.children?.length === 1 && child?.tagName === 'img') {
      const imgProps = child.properties || {};
      return <ChatImage src={imgProps.src} alt={imgProps.alt} />;
    }
    return <p {...props}>{children}</p>;
  },
  img: ({ src, alt }) => <ChatImage src={src} alt={alt} />,
};
