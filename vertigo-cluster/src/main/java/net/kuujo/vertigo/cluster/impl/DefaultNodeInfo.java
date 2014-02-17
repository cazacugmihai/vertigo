package net.kuujo.vertigo.cluster.impl;

import java.util.Collection;

import net.kuujo.vertigo.cluster.AssignmentInfo;
import net.kuujo.vertigo.cluster.NodeInfo;

/**
 * Default node info implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultNodeInfo implements NodeInfo {
  private String address;
  private Collection<AssignmentInfo> assignments;

  @Override
  public String address() {
    return address;
  }

  @Override
  public Collection<AssignmentInfo> assignments() {
    return assignments;
  }

}
