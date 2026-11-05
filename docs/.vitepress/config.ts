import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: 'Valkeyrie',
  description: 'Type-safe key-value store with atomic transactions, Standard Schema validation and pluggable drivers',
  base: '/valkeyrie/',

  head: [
    ['link', { rel: 'icon', type: 'image/png', href: '/valkeyrie/logo.png' }],
    ['meta', { name: 'theme-color', content: '#d97706' }],
    ['meta', { name: 'og:type', content: 'website' }],
    ['meta', { name: 'og:locale', content: 'en' }],
    ['meta', { name: 'og:site_name', content: 'Valkeyrie' }],
    ['meta', { name: 'og:image', content: 'https://ducktors.github.io/valkeyrie/logo.png' }],
  ],

  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    logo: '/logo.png',

    outline: {
      level: [2, 3],
      label: 'On this page'
    },

    nav: [
      { text: 'Guide', link: '/guides/getting-started' },
      { text: 'API Reference', link: '/api/api-reference' },
    ],

    sidebar: {
      '/guides/': [
        {
          text: 'Guide',
          items: [
            { text: 'Getting Started', link: '/guides/getting-started' },
            { text: 'Schema Validation', link: '/guides/schema-validation' },
            { text: 'Factory Methods', link: '/guides/factory-methods' },
            { text: 'Serializers', link: '/guides/serializers' },
            { text: 'Advanced Patterns', link: '/guides/advanced-patterns' },
          ]
        }
      ],
      '/api/': [
        {
          text: 'API Reference',
          items: [
            { text: 'API Overview', link: '/api/api-reference' },
            { text: 'Types', link: '/api/types' },
            { text: 'Generated API', link: '/api/generated/' },
          ]
        }
      ]
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/ducktors/valkeyrie' }
    ],

    search: {
      provider: 'local'
    },

    editLink: {
      pattern: 'https://github.com/ducktors/valkeyrie/edit/main/docs/:path',
      text: 'Edit this page on GitHub'
    },

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2025 Ducktors'
    }
  },

  lastUpdated: true,

  markdown: {
    lineNumbers: true,
    theme: {
      light: 'github-light',
      dark: 'github-dark'
    }
  }
})
