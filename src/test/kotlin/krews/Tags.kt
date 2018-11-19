package krews

import io.kotlintest.Tag

// Tag for unit tests - tests that are completely self contained
object Unit : Tag()

// Tag for integration tests - tests that will run on a single system, but involve interacting with other programs
object Integration : Tag()

// Tag for end to end tests - tests that will interact with external services
object E2E : Tag()
