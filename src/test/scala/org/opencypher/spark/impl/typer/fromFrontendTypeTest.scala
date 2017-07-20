/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark.impl.typer

import org.neo4j.cypher.internal.frontend.v3_2.{symbols => frontend}
import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTFloat, CTInteger, CTNumber}

class fromFrontendTypeTest extends BaseTestSuite {

  test("should convert basic types") {
    fromFrontendType(frontend.CTBoolean) shouldBe CTBoolean
    fromFrontendType(frontend.CTInteger) shouldBe CTInteger
    fromFrontendType(frontend.CTFloat) shouldBe CTFloat
    fromFrontendType(frontend.CTNumber) shouldBe CTNumber
  }
}
