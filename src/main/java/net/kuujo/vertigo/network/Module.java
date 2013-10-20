package net.kuujo.vertigo.network;

import net.kuujo.vertigo.context.ComponentContext;

import org.vertx.java.core.json.JsonObject;

/**
 * A module component.
 *
 * @author Jordan Halterman
 */
public class Module extends Component<Module> {

  public static final String MODULE = "module";

  public Module() {
    super();
  }

  public Module(String address) {
    super(address);
  }

  @Override
  public String getType() {
    return MODULE;
  }

  /**
   * Returns the module name.
   *
   * @return
   *   The module name.
   */
  public String getModule() {
    return definition.getString(MODULE);
  }

  /**
   * Sets the module name.
   *
   * @param moduleName
   *   The module name.
   * @return
   *   The called module component.
   */
  public Module setModule(String moduleName) {
    definition.putString(MODULE, moduleName);
    return this;
  }

  @Override
  public ComponentContext createContext() throws MalformedNetworkException {
    JsonObject context = super.createJsonContext();
    context.putString(TYPE, Component.MODULE);
    String moduleName = context.getString(MODULE);
    if (moduleName == null) {
      throw new MalformedNetworkException("Invalid module component. No module name defined.");
    }
    return ComponentContext.fromJson(context);
  }

}