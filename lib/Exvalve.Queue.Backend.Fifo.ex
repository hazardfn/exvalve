################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve.Queue.Backend.Fifo do
  @behaviour Exvalve.Queue
  use GenServer

  @moduledoc """
  The fifo backend provides a first in first out behaviour for valvex
  it uses a standard erlang queue - if this behaviour does not suit
  your application or your wish to use a custom queue feel free
  to create your own backend, be careful to ensure you adhere to
  the behaviour of a valvex queue and to include knowledge of
  tombstoning and locking to your backend
  """

  @doc """
  Consumes N number of items from the queue, in this backend N is determined
  by the poll_count parameter
  """
  @spec consume(Exvalve.exvalve_ref(),
    Exvalve.exvalve_ref(),
    Exvalve.queue_backend(),
    Exvalve.queue_key(),
    non_neg_integer(),
    non_neg_integer()) :: :ok
  def consume(exvalve, qpid, backend, key, timeout, poll_count) do
    #do_consume(exvalve, qpid, backend, key, timeout, poll_count)
    :lol
  end

  @doc """
  Begins a crossover; the replacement of the current queue settings
  with a different arrangment
  """
  @spec crossover( Exvalve.exvalve_ref(), Exvalve.exvalve_queue()) :: :ok
  def crossover(q, nuq) do
    GenServer.cast(q, {:crossover, nuq})
    GenServer.cast(q, :restart_consumer)
  end

  @doc """
  Returns the queue state
  """
  @spec get_state(Exvalve.exvalve_ref()) :: Exvalve.Queue.queue_state()
  def get_state(q) do
    GenServer.call(q, :get_state)
  end

  @doc """
  Returns true if the consumer is active
  """
  @spec is_consuming(Exvalve.exvalve_ref()) :: true | false
  def is_consuming(q) do
    GenServer.call(q, :is_consuming)
  end

  @doc """
  Returns true if the queue is locked, more details about locking
  are in the readme
  """
  @spec is_locked(Exvalve.exvalve_ref()) :: true | false
  def is_locked(q) do
    GenServer.call(q, :is_locked)
  end

  @doc """
  Returns true if the queue is marked for deletion, it will
  only be deleted once empty and can only be guaranteed to be
  eventually empty if locked and currently being consumed
  """
  @spec is_tombstoned(Exvalve.exvalve_ref()) :: true | false
  def is_tombstoned(q) do
    GenServer.call(q, :is_tombstoned)
  end

  @doc """
  Lock the queue, refer to the readme for specifics as to what this means
  """
  @spec lock(Exvalve.exvalve_ref()) :: :ok
  def lock(q) do
    GenServer.cast(q, :lock)
  end

  @doc """
  pop the queue, in the case of this backend it's first in first out
  """
  @spec pop(Exvalve.exvalve_ref()) :: Exvalve.exvalve_queue_item()
  def pop(q) do
    GenServer.call(q, :pop)
  end

  @doc """
  pop_r is the reverse option of pop, in this the last in will be popped
  """
  @spec pop_r(Exvalve.exvalve_ref()) :: Exvalve.exvalve_queue_item()
  def pop_r(q) do
    GenServer.call(q, :pop_r)
  end

  @doc """
  Pushes a queue item to the queue. The item is placed at the end
  """
  @spec push(Exvalve.exvalve_ref(), Exvalve.exalve_queue_item()) :: :ok
  def push(q, value) do
    GenServer.cast(q, {:push, value})
  end

  @doc """
  Pushes a queue item to the queue. The item is placed at the front
  """
  @spec push_r(Exvalve.exvalve_ref(), Exvalve.exalve_queue_item()) :: :ok
  def push_r(q, value) do
    GenServer.cast(q, {:push_r, value})
  end

  @doc """
  Returns the current number of items in the queue
  """
  @spec size(Exvalve.exvalve_ref()) :: non_neg_integer()
  def size(q) do
    GenServer.call(q, :size)
  end

  @doc """
  Starts a timer that regularly calls the consume function
  to facilitate steady and constant slurping of the queue,
  how regularly is determined by the poll_rate. How many
  items per call is determined by the poll_count
  """
  @spec start_consumer(Exvalve.exvalve_ref()) :: :ok
  def start_consumer(q) do
    GenServer.cast(q, :start_consumer)
  end

  @doc """
  Starts a link with the queue gen_server
  """
  @spec start_link(Exvalve.exvalve_ref(), Exvalve.exvalve_queue()) :: Exvalve.exvalve_ref()
  def start_link(exvalve, {key, _, _, _, _, _, _} = q) do
    GenServer.start_link(__MODULE__, [exvalve, q], name: key)
  end

  @doc """
  Stops the regular consumption of the queue
  """
  @spec stop_consumer(Exvalve.exvalve_ref()) :: :ok
  def stop_consumer(q) do
    GenServer.cast(q, :stop_consumer)
  end

  @doc """
  Marks the queue for deletion
  """
  @spec tombstone(Exvalve.exvalve_ref()) :: :ok
  def tombstone(q) do
    GenServer.cast(q, :tombstone)
  end

  @doc """
  Unlocks the queue, for more information about what this means
  consult the readme
  """
  @spec unlock(Exvalve.exvalve_ref()) :: :ok
  def unlock(q) do
    GenServer.cast(q, :unlock)
  end

  ##==============================================================================
  ## GenServer callbacks
  ##==============================================================================
  def init([ exvalve, {key,
                       {:threshold, threshold},
                       {:timeout, timeout, :seconds},
                       {:pushback, pushback, :seconds},
                       {:poll_rate, poll_rate, :ms},
                       {:poll_count, poll_count},
                       backend
                      } = q]) do
    #Exvalve.notify(exvalve, {:queue_started, q})
    {:ok, %Exvalve.Queue{:key => key,
                         :threshold => threshold,
                         :timeout => timeout,
                         :pushback => pushback,
                         :backend => backend,
                         :poll_rate => poll_rate,
                         :poll_count => poll_count,
                         :q => q,
                         :exvalve => exvalve
                        }}
  end
end
