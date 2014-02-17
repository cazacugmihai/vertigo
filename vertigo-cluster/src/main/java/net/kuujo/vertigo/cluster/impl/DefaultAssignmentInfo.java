/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.vertigo.cluster.impl;

import net.kuujo.vertigo.cluster.AssignmentInfo;
import net.kuujo.vertigo.cluster.InstanceInfo;

/**
 * Default assignment info implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultAssignmentInfo implements AssignmentInfo {
  private InstanceInfo instance;

  @Override
  public InstanceInfo instance() {
    return instance;
  }

  @Override
  public String toString() {
    return instance.id();
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof AssignmentInfo && ((AssignmentInfo) obj).instance().id().equals(instance().id()));
  }

}
