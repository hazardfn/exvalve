################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve.Supervisor do
  use Supervisor
  import Supervisor.Spec

  ##==============================================================================
  ## Api
  ##==============================================================================

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
    Exvalve.Queue.Supervisor.start_link()
  end

  def start_link(options) do
    Supervisor.start_link(__MODULE__, options, name: __MODULE__)
    Exvalve.Queue.Supervisor.start_link()
  end

  ##==============================================================================
  ## Supervisor callbacks
  ##==============================================================================

  def init([]) do
    supervise(exvalve_server_child_specs, strategy: :one_for_one, max_restarts: 3, max_seconds: 60)
  end

  def init(options) do
    supervise(exvalve_server_child_specs_with_options(options), strategy: :one_for_one, max_restarts: 3, max_seconds: 60)
  end

  ##==============================================================================
  ## Private
  ##==============================================================================
  defp exvalve_server_child_specs() do
    [worker(Exvalve.Server,
      [[]],
      id: :exvalve,
      restart: :permanent,
      shutdown: 3600,
      modules: [Exvalve.Server]
      )]
  end

  defp exvalve_server_child_specs_with_options(options) do
    [worker(Exvalve,
      [options],
      id: :exvalve,
      restart: :permanent,
      shutdown: 3600,
      modules: [Exvalve.Server]
    )]
  end
end
