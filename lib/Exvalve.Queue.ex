################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve.Queue do

  defstruct key: nil,
    threshold: nil,
    timeout: nil,
    pushback: nil,
    backend: nil,
    size: 0,
    poll_rate: nil,
    poll_count: nil,
    queue: :queue.new(),
    q: nil,
    locked: false,
    tombstoned: false,
    exvalve: nil,
    queue_pid: self(),
    consumer: nil,
    consuming: false

  ##==============================================================================
  ## Types
  ##==============================================================================

  @typedoc """
  The structure that represents a queues state
  """
  @type queue_state :: %Exvalve.Queue{ :key => Exvalve.queue_key(),
                                       :threshold => non_neg_integer(),
                                       :timeout => non_neg_integer(),
                                       :pushback => non_neg_integer(),
                                       :backend => module(),
                                       :size => non_neg_integer(),
                                       :poll_rate => non_neg_integer(),
                                       :poll_count => non_neg_integer(),
                                       :queue => :queue.queue(),
                                       :q => Exvalve.exvalve_queue(),
                                       :locked => true | false,
                                       :tombstoned => true | false,
                                       :exvalve => Exvalve.exvalve_ref(),
                                       :queue_pid => Exvalve.exvalve_ref(),
                                       :consumer => nil | :timer.tref(),
                                       :consuming => true | false }

  ##==============================================================================
  ## Callbacks
  ##==============================================================================

  @callback get_state(q :: Exvalve.exvalve_ref()) :: queue_state()
  @callback is_consuming(q :: Exvalve.exvalve_ref()) :: true | false
  @callback is_locked(q :: Exvalve.exvalve_ref()) :: true | false
  @callback is_tombstoned(q :: Exvalve.exvalve_ref()) :: true | false
  @callback lock(q :: Exvalve.exvalve_ref()) :: :ok
  @callback pop(q :: Exvalve.exvalve_ref()) :: any()
  @callback pop_r(q :: Exvalve.exvalve_ref()) :: any()
  @callback push(q :: Exvalve.exvalve_ref(), value :: Exvalve.exvalve_queue_item()) :: :ok
  @callback push_r(q :: Exvalve.exvalve_ref(), value :: Exvalve.exvalve_queue_item()) :: :ok
  @callback size(q :: Exvalve.exvalve_ref()) :: non_neg_integer()
  @callback start_consumer(q :: Exvalve.exvalve_ref()) :: :ok
  @callback start_link(exvalve :: Exvalve.exvalve_ref(), q :: Exvalve.exvalve_queue()) :: Exvalve.exvalve_ref()
  @callback stop_consumer(q :: Exvalve.exvalve_ref()) :: :ok
  @callback tombstone(q :: Exvalve.exvalve_ref()) :: :ok
  @callback unlock(q :: Exvalve.exvalve_ref()) :: :ok

  ##==============================================================================
  ## API
  ##==============================================================================

  @doc """
  This defines the crossover behaviour for your queue; should you be
  developing your own backend take a look at how it is done in other stock
  modules - if your module cannot work with crossover for any reason it is
  important that the user is made aware and something is logged
  """
  @spec crossover(Exvalve.queue_backend(), Exvalve.exvalve_ref(), Exvalve.exvalve_queue()) :: :ok
  def crossover(backend, q, nuq) do
    backend.crossover(q, nuq)
  end

  @doc """
  Returns the state of the queue
  """
  @spec get_state(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: queue_state()
  def get_state(backend, q) do
    backend.get_state(q)
  end

  @doc """
  Returns true if the consumer is started, false otherwise
  """
  @spec is_consuming(Exvalve.queue_backend, Exvalve.exvalve_ref()) :: true | false
  def is_consuming(backend, q) do
    backend.is_consuming(q)
  end

  @doc """
  This should reply true or false depending on if the queue is locked or not;
  a locked queue is one that cannot be pushed to, remaining work may continue
  to be consumed however. View the stock backends or the readme for more info
  """
  @spec is_locked(Exvalve.queue_backend, Exvalve.exvalve_ref()) :: true | false
  def is_locked(backend, q) do
    backend.is_locked(q)
  end

  @doc """
  This should reply true or false depending on if the queue is tombstoned or
  not; a tombstoned queue is flagged for deletion and will delete itself
  when all work is consumed. A tombstoned queue does not have to be locked,
  this simply means it will stay alive as long as work is being passed to it.
  """
  @spec is_tombstoned(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: true | false
  def is_tombstoned(backend, q) do
    backend.is_tombstoned(q)
  end

  @doc """
  This will lock a queue, a locked queue cannot be pushed to
  """
  @spec lock(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: :ok
  def lock(backend, q) do
    backend.lock(q)
  end

  @doc """
  This will pop the queue, depending on your backends behaviour
  this can have different conotations, see the documentation for
  your specific backend for more info.
  """
  @spec pop(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: any()
  def pop(backend, q) do
    backend.pop(q)
  end

  @doc """
  This will pop the queue but with the reverse operation of pop
  depending on your backends behaviour this can have different
  conotations, see the documentation for your specific backend for more info.
  """
  @spec pop_r(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: any()
  def pop_r(backend, q) do
    backend.pop_r(q)
  end

  @doc """
  This will push a queue item to the queue, this can have different
  conotations depending on your backend, view it's documentation
  for more information.
  """
  @spec push(Exvalve.queue_backend(), Exvalve.exvalve_ref(), Exvalve.exvalve_queue_item()) :: :ok
  def push(backend, q, value) do
    backend.push(q, value)
  end

  @doc """
  This will do the reverse operation of push, this can have different
  conotations depending on your backend, view it's documentation
  for more information.
  """
  @spec push_r(Exvalve.queue_backend(), Exvalve.exvalve_ref(), Exvalve.exvalve_queue_item()) :: :ok
  def push_r(backend, q, value) do
    backend.push_r(q, value)
  end

  @doc """
  This function receives the current size of the queue, if for some reason this
  is not possible or costly with your implementation make sure the user is notified
  this doesn't work and return a 0
  """
  @spec size(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: non_neg_integer()
  def size(backend, q) do
    backend.size(q)
  end

  @doc """
  This function should start your consumer, the consumer should be a function
  that is run at intervals to consume the queue - it should be aware of the
  poll_rate and poll_count of the queue
  """
  @spec start_consumer(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: :ok
  def start_consumer(backend, q) do
    backend.start_consumer(q)
  end

  @spec start_link(Exvalve.queue_backend(), Exvalve.exvalve_ref(), Exvalve.exvalve_queue()) :: {:ok, Exvalve.exvalve_ref()}
  def start_link(backend, exvalve, q) do
    backend.start_link(exvalve, q)
  end

  @doc """
  This function should stop your consumer, the timer consuming your queue
  at regular intervals
  """
  @spec stop_consumer(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: :ok
  def stop_consumer(backend, q) do
    backend.stop_consumer(q)
  end

  @doc """
  This function should mark your queue for deletion, once the queue is empty it should
  be removed, this is accompolished by having your consumer check for tombstoned
  status
  """
  @spec tombstone(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: :ok
  def tombstone(backend, q) do
    backend.tombstone(q)
  end

  @doc """
  This function unlocks as locked queue.
  """
  @spec unlock(Exvalve.queue_backend(), Exvalve.exvalve_ref()) :: :ok
  def unlock(backend, q) do
    backend.unlock(q)
  end

end
