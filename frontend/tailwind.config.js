import typography from '@tailwindcss/typography';

/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        vantyra: {
          bg:        '#1a1a2e',
          'bg-s':    '#252540',
          'bg-h':    '#2d2d50',
          text:      '#e8e8f0',
          'text-s':  '#8888a0',
          accent:    '#00D4FF',
          'accent-s':'#7B61FF',
          success:   '#34D399',
          error:     '#F87171',
          border:    '#3a3a5c',
        },
      },
    },
  },
  plugins: [typography],
}
