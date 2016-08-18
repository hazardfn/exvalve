################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve.Worker do
  require Logger
  use GenServer

  defstruct exvalve: nil

  ##==============================================================================
  ## API Functions
  ##==============================================================================
  @doc """
  Starts a worker.
  """
  @spec start_link(Exvalve.exvalve_ref()) :: Exvalve.exvalve_ref()
  def start_link(exvalve) do
    {:ok, pid} = GenServer.start_link(__MODULE__, exvalve)
    pid
  end

  @doc """
  Pass some work onto the worker.
  """
  @spec do_work(Exvalve.exvalve_ref(), function()) :: :ok
  def do_work(worker, work) do
    GenServer.cast(worker, work)
  end

  @doc """
  Stop the worker
  """
  def stop(worker) do
    GenServer.call(worker, :stop)
  end

  ##==============================================================================
  ## GenServer Callbacks
  ##==============================================================================
  def init(exvalve) do
    {:ok, %Exvalve.Worker{ exvalve: exvalve }}
  end

  def handle_cast({:work, work}, s = %Exvalve.Worker{ exvalve: exvalve}) do
    work.()
    GenServer.cast(exvalve, {:work_finished, self()})
    {:noreply, s}
  end

  def handle_call(:stop, _from, s = %Exvalve.Worker{ exvalve: exvalve}) do
    Exvalve.notify(exvalve, {:worker_stopped, self()})
    {:stop, :normal, :ok, s}
  end
end
