import { describe, expect, it } from 'vitest'
import { greet, sum } from './index'

describe('greet function', () => {
  it('should greet a person by name', () => {
    expect(greet('Alice')).toBe('Hello, Alice!')
  })

  it('should greet with an empty string', () => {
    expect(greet('')).toBe('Hello, !')
  })
})

describe('sum function', () => {
  it('should sum an array of numbers', () => {
    expect(sum([1, 2, 3, 4, 5])).toBe(15)
  })

  it('should return 0 for an empty array', () => {
    expect(sum([])).toBe(0)
  })

  it('should handle negative numbers', () => {
    expect(sum([-1, -2, -3])).toBe(-6)
  })

  it('should handle a single number', () => {
    expect(sum([42])).toBe(42)
  })
}) 