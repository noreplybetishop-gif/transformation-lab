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
import lesson35 from './lesson35'
import lesson36 from './lesson36'
import lesson37 from './lesson37'
import lesson38 from './lesson38'
import lesson39 from './lesson39'
import lesson40 from './lesson40'
import lesson41 from './lesson41'
import lesson42 from './lesson42'
import lesson43 from './lesson43'
import lesson44 from './lesson44'
import lesson45 from './lesson45'
import lesson46 from './lesson46'
import lesson47 from './lesson47'
import lesson48 from './lesson48'
import lesson49 from './lesson49'
import lesson50 from './lesson50'
import lesson51 from './lesson51'
import lesson52 from './lesson52'
import lesson53 from './lesson53'
import lesson54 from './lesson54'
import lesson55 from './lesson55'
import lesson56 from './lesson56'
import lesson57 from './lesson57'
import lesson58 from './lesson58'
import lesson59 from './lesson59'

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
  lesson35,
  lesson36,
  lesson37,
  lesson38,
  lesson39,
  lesson40,
  lesson41,
  lesson42,
  lesson43,
  lesson44,
  lesson45,
  lesson46,
  lesson47,
  lesson48,
  lesson49,
  lesson50,
  lesson51,
  lesson52,
  lesson53,
  lesson54,
  lesson55,
  lesson56,
  lesson57,
  lesson58,
  lesson59,
]

export const BASICS_LESSONS_COUNT = 14
export const INTERMEDIATE_LESSONS_COUNT = 20
export const ADVANCED_LESSONS_COUNT = 25

export function isBasicsLesson(id: number): boolean {
  return id >= 1 && id <= 14
}

export function isIntermediateLesson(id: number): boolean {
  return id >= 15 && id <= 34
}

export function isAdvancedLesson(id: number): boolean {
  return id >= 35 && id <= 59
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
