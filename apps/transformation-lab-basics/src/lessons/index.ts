import type { Lesson } from '../engine/types'
import lesson00 from './lesson00'
import lesson01 from './lesson01'
import lesson02 from './lesson02'
import lesson03 from './lesson03'
import lesson04 from './lesson04'
import lesson05 from './lesson05'
import lesson06 from './lesson06'
import lesson07 from './lesson07'
import lesson08 from './lesson08'
import lesson09 from './lesson09'
import lesson10 from './lesson10'
import lesson11 from './lesson11'
import lesson12 from './lesson12'
import lesson13 from './lesson13'
import lesson14 from './lesson14'
import lesson15 from './lesson15'
import lesson16 from './lesson16'
import lesson17 from './lesson17'
import lesson18 from './lesson18'
import lesson19 from './lesson19'
import lesson20 from './lesson20'
import lesson21 from './lesson21'
import lesson22 from './lesson22'
import lesson23 from './lesson23'
import lesson24 from './lesson24'
import lesson25 from './lesson25'
import lesson26 from './lesson26'
import lesson27 from './lesson27'
import lesson28 from './lesson28'
import lesson29 from './lesson29'
import lesson30 from './lesson30'
import lesson31 from './lesson31'
import lesson32 from './lesson32'
import lesson33 from './lesson33'
import lesson34 from './lesson34'

export const lessons: Lesson[] = [
  lesson00,
  lesson01,
  lesson02,
  lesson03,
  lesson04,
  lesson05,
  lesson06,
  lesson07,
  lesson08,
  lesson09,
  lesson10,
  lesson11,
  lesson12,
  lesson13,
  lesson14,
  lesson15,
  lesson16,
  lesson17,
  lesson18,
  lesson19,
  lesson20,
  lesson21,
  lesson22,
  lesson23,
  lesson24,
  lesson25,
  lesson26,
  lesson27,
  lesson28,
  lesson29,
  lesson30,
  lesson31,
  lesson32,
  lesson33,
  lesson34,
]

export const BASICS_LESSONS_COUNT = 14
export const INTERMEDIATE_LESSONS_COUNT = 20

export function isBasicsLesson(id: number): boolean {
  return id >= 1 && id <= 14
}

export function isIntermediateLesson(id: number): boolean {
  return id >= 15 && id <= 34
}

export function getLessonById(id: number): Lesson | undefined {
  return lessons.find((l) => l.id === id)
}

export function getLastLessonId(): number {
  return lessons.reduce((max, l) => (l.id > max ? l.id : max), 0)
}

/** Stable key for a task's progress entry: `<lessonId>.<taskId>`. */
export function taskKey(lessonId: number, taskId: string): string {
  return `${lessonId}.${taskId}`
}
