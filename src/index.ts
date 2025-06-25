/**
 * A simple example function for the TypeScript library starter
 * @param name - The name to greet
 * @returns A greeting message
 */
export function greet(name: string): string {
  return `Hello, ${name}!`
}

/**
 * An example utility function
 * @param numbers - Array of numbers to sum
 * @returns The sum of all numbers
 */
export function sum(numbers: number[]): number {
  return numbers.reduce((acc, num) => acc + num, 0)
}

/**
 * Default export example
 */
export default {
  greet,
  sum,
}