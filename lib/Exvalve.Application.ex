################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve.Application do
  use Application

  ##==============================================================================
  ## Api
  ##==============================================================================

  def start(_type, _args) do
    case Application.get_all_env(:exvalve) do
      [] -> Exvalve.Supervisor.start_link()
      _Options ->
        queues = get_setting(:exvalve, :queues)
        event_handlers = get_setting(:exvalve, :event_handlers)
        workers = get_setting(:exvalve, :workers)
        pushback_enabled = get_setting(:exvalve, :pushback_enabled)
        Exvalve.Supervisor.start_link([ {:queues, queues},
                                        {:pushback_enabled, pushback_enabled},
                                        {:workers, workers},
                                        {:event_handlers, event_handlers}
                                      ])
    end
  end

  ##==============================================================================
  ## Private
  ##==============================================================================

  defp get_setting(app, par) do
    Application.get_env(app, par)
  end
end
