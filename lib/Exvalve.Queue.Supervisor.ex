################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve.Queue.Supervisor do
  use Supervisor
  import Supervisor.Spec

  ##==============================================================================
  ## API Functions
  ##==============================================================================

  @doc """
  Starts the supervisor
  """
  @spec start_link() :: {:ok, Exvalve.exvalve_ref()}
  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Used to start a new supervised queue
  """
  @spec start_child(list()) :: Supervisor.startchild_ret()
  def start_child([_backend, _key, _q] = args) do
    Supervisor.start_child(__MODULE__, exvalve_queue_child_specs(args))
  end

  ##==============================================================================
  ## Supervisor callbacks
  ##==============================================================================

  def init([]) do
    supervise([], strategy: :one_for_one, max_restarts: 3, max_seconds: 60)
  end

  defp exvalve_queue_child_specs([backend, key, q]) do
    [worker(key,
        [backend, :exvalve, q],
        id: key,
        function: Exvalve.Queue.start_link(backend, :exvalve, q),
        restart: :permanent,
        shutdown: 3600,
        modules: [backend]
      )]
  end
end
