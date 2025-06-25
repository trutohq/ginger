# TypeScript Library Starter

[![npm version](https://badge.fury.io/js/my-typescript-library.svg)](https://badge.fury.io/js/my-typescript-library)
[![CI](https://github.com/username/my-typescript-library/actions/workflows/ci.yml/badge.svg)](https://github.com/username/my-typescript-library/actions/workflows/ci.yml)
[![TypeScript](https://img.shields.io/badge/TypeScript-Ready-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**A modern TypeScript library starter template with best practices and tooling.**

This template provides a solid foundation for building TypeScript libraries with modern tooling, testing, and development workflows.

## ✨ Features

- 🔧 **TypeScript**: Full TypeScript support with strict configuration
- 🧪 **Testing**: Vitest for fast and modern testing
- 📦 **Bundling**: Bun for fast builds and development
- 🎨 **Linting**: ESLint with TypeScript and Prettier integration
- 🔄 **CI/CD**: GitHub Actions workflow ready
- 📝 **Changesets**: Automated versioning and changelog generation
- 🪝 **Git Hooks**: Husky and lint-staged for pre-commit validation
- 🌍 **Universal**: ESM modules with proper type declarations

## 📦 Installation

```bash
npm install my-typescript-library
```

```bash
yarn add my-typescript-library
```

```bash
pnpm add my-typescript-library
```

```bash
bun add my-typescript-library
```

## 🚀 Quick Start

```typescript
import { myFunction } from 'my-typescript-library'

// Your library usage here
const result = myFunction()
console.log(result)
```

## 📖 API Reference

### `myFunction()`

Description of your main function.

```typescript
const result = myFunction()
```

## 🛠️ Development

### Prerequisites

- [Bun](https://bun.sh/) (recommended) or Node.js 18+

### Setup

```bash
# Clone the repository
git clone https://github.com/username/my-typescript-library.git
cd my-typescript-library

# Install dependencies
bun install

# Start development mode
bun run dev
```

### Available Scripts

```bash
# Development
bun run dev          # Start development mode with watch
bun run build        # Build the library
bun run typecheck    # Run TypeScript type checking

# Testing
bun test             # Run tests
bun run test:coverage # Run tests with coverage
bun run test:ui      # Run tests with UI

# Linting and Formatting
bun run lint         # Run ESLint
bun run lint:fix     # Fix ESLint issues
bun run format       # Format code with Prettier
bun run format:check # Check formatting

# Publishing
bun run prepublishOnly # Pre-publish checks (typecheck, lint, test, build)
```

### Project Structure

```
├── src/
│   └── index.ts          # Main entry point
├── dist/                 # Built files (auto-generated)
├── tests/                # Test files
├── package.json          # Package configuration
├── tsconfig.json         # TypeScript configuration
├── vitest.config.ts      # Vitest configuration
├── eslint.config.js      # ESLint configuration
└── README.md             # This file
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [TypeScript](https://www.typescriptlang.org/)
- Tested with [Vitest](https://vitest.dev/)
- Bundled with [Bun](https://bun.sh/)
