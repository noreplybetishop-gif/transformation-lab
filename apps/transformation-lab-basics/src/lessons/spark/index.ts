import type { Lesson } from '../../engine/types'
import sparkLesson01 from './sparkLesson01'
import sparkLesson02 from './sparkLesson02'
import sparkLesson03 from './sparkLesson03'
import sparkLesson04 from './sparkLesson04'
import sparkLesson05 from './sparkLesson05'
import sparkLesson06 from './sparkLesson06'
import sparkLesson07 from './sparkLesson07'
import sparkLesson08 from './sparkLesson08'
import sparkLesson09 from './sparkLesson09'
import sparkLesson10 from './sparkLesson10'
import sparkLesson11 from './sparkLesson11'
import sparkLesson12 from './sparkLesson12'
import sparkLesson13 from './sparkLesson13'
import sparkLesson14 from './sparkLesson14'
import sparkLesson15 from './sparkLesson15'
import sparkLesson16 from './sparkLesson16'
import sparkLesson17 from './sparkLesson17'
import sparkLesson18 from './sparkLesson18'
import sparkLesson19 from './sparkLesson19'
import sparkLesson20 from './sparkLesson20'
import sparkLesson21 from './sparkLesson21'
import sparkLesson22 from './sparkLesson22'
import sparkLesson23 from './sparkLesson23'
import sparkLesson24 from './sparkLesson24'
import sparkLesson25 from './sparkLesson25'
import sparkLesson26 from './sparkLesson26'
import sparkLesson27 from './sparkLesson27'

export const sparkLessons: Lesson[] = [
  sparkLesson01,
  sparkLesson02,
  sparkLesson03,
  sparkLesson04,
  sparkLesson05,
  sparkLesson06,
  sparkLesson07,
  sparkLesson08,
  sparkLesson09,
  sparkLesson10,
  sparkLesson11,
  sparkLesson12,
  sparkLesson13,
  sparkLesson14,
  sparkLesson15,
  sparkLesson16,
  sparkLesson17,
  sparkLesson18,
  sparkLesson19,
  sparkLesson20,
  sparkLesson21,
  sparkLesson22,
  sparkLesson23,
  sparkLesson24,
  sparkLesson25,
  sparkLesson26,
  sparkLesson27,
]

export const SPARK_BASICS_COUNT = 12
export const SPARK_INTERMEDIATE_COUNT = 15

export function isSparkLesson(id: number): boolean {
  return id >= 101 && id <= 199
}

export function isSparkBasicsLesson(id: number): boolean {
  return id >= 101 && id <= 112
}

export function isSparkIntermediateLesson(id: number): boolean {
  return id >= 113 && id <= 127
}

export function getSparkLessonById(id: number): Lesson | undefined {
  return sparkLessons.find((l) => l.id === id)
}
