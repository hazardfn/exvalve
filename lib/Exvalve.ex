################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve do
  require Logger

  @moduledoc """
  This module provides the main interface to exvalve. For more information
  refer to the readme
  """

  ##==============================================================================
  ## TYPES
  ##==============================================================================

  ## Queue Types -----------------------------------------------------------------
  @typedoc """
  A queue backend module.
  """
  @type queue_backend :: module

  @typedoc """
  An atom representing a queue key.
  """
  @type queue_key :: atom

  @typedoc """
  The poll rate of a consumer - the amount of times consume is called per ms.
  """
  @type queue_poll_rate :: {:poll_rate, non_neg_integer(), :ms}

  @typedoc """
  The number of items to be harvested each poll_rate
  """
  @type queue_poll_count :: {:poll_count, non_neg_integer()}

  @typedoc """
  The amount of seconds we should pushback on the client in case of overload.
  """
  @type queue_pushback :: {:pushback, non_neg_integer(), :seconds}

  @typedoc """
  The amount of items before the queue is considered overloaded.
  """
  @type queue_threshold :: {:threshold, non_neg_integer()}

  @typedoc """
  The amount of time an item may sit in the queue before it's stale
  """
  @type queue_timeout :: {:timeout, non_neg_integer(), :seconds}

  ## Option / Behavioural modifier types -------------------------------------------
  @typedoc """
  Different options for the the behaviour of the add call
  """
  @type add_option :: :crossover_on_existing | :manual_start | nil

  @typedoc """
  Different options for the behaviour of the remove call
  """
  @type remove_option :: :force_remove | :lock_queue | nil

  ## Error types  ------------------------------------------------------------------
  @typedoc """
  The error produced when a queue key cannot be found
  """
  @type key_find_error :: {:error, :key_not_found}

  @typedoc """
  The error produced when a key is not unique
  """
  @type unique_key_error :: {:error, :key_not_unique}

  ## Other types -------------------------------------------------------------------
  @typedoc """
  The type used to represent references in exvalve
  """
  @type exvalve_ref :: pid() | atom()

  @typedoc """
  Represents the structure of a queue in exvalve
  """
  @type exvalve_queue :: { queue_key(),
                           queue_threshold(),
                           queue_timeout(),
                           queue_pushback(),
                           queue_poll_rate(),
                           queue_poll_count(),
                           queue_backend() }

  @typedoc """
  Represents the type of the item that is placed on the queues
  """
  @type exvalve_queue_item :: {function(), :erlang.timestamp()}

  @typedoc """
  Loose representation of the exvalve config
  """
  @type exvalve_options :: [{atom(), any()}]

  @typedoc """
  Represents a list of exvalve workers
  """
  @type exvalve_workers() :: [pid()]

  ##==============================================================================
  ## API Functions
  ##==============================================================================
  @doc """
  Starts a link to the exvalve gen_server
  """
  @spec start_link(exvalve_options()) :: {:ok, exvalve_ref()}
  def start_link(options) do
    pid = Exvalve.Server.start_link(options)
    Logger.info "Exvalve server started: #{:erlang.pid_to_list(pid)}"
    {:ok, pid}
  end

  @doc """
  Adds a queue to exvalve using the default option nil.
  """
  @spec add(exvalve_ref(), exvalve_queue()) :: :ok | unique_key_error()
  def add(exvalve, { _key,
                     {:threshold, _threshold},
                     {:timeout, _timeout, :seconds},
                     {:pushback, _pushback, :seconds},
                     {:poll_rate, _poll, :ms},
                     {:poll_count, _pollCount},
                     _backend
                   } = q) do
    Exvalve.add(exvalve, q, nil)
  end

  @doc """
  Adds a queue to exvalve. There are a few options that alter slightly
  the behaviour of the add:

  :manual_start - adds the queue but does not immediately start the consumer
  :crossover_on_existing - adds the queue, if a queue of the same key exists it will swap out the old settings
  nil - default behaviour which is to add the queue or error if the key isn't unique
  """
  @spec add(exvalve_ref(), exvalve_queue(), add_option()) :: :ok | unique_key_error()
  def add(exvalve, { _key,
                     {:threshold, _threshold},
                     {:timeout, _timeout, :seconds},
                     {:pushback, _pushback, :seconds},
                     {:poll_rate, _poll, :ms},
                     {:poll_count, _pollCount},
                     _backend
                   } = q, option) do
    Exvalve.Server.add(exvalve, q, option)
  end

  @doc """
  Removes a queue from exvalve with the default setting.
  """
  @spec remove(exvalve_ref(), queue_key()) :: :ok | key_find_error()
  def remove(exvalve, key) do
    Exvalve.remove(exvalve, key, nil)
  end

  @doc """
  Removes a queue from exvalve. There are a few options that alter the way
  a remove behaves:

  :force_remove - Removes the queue by force regardless of items in it
  :lock_queue - Locks the queue and marks it for deletion meaning it will be removed when empty
  nil - Marks the queue for deletion but new items can flow in, it won't be removed until empty
  """
  @spec remove(exvalve_ref(), queue_key(), remove_option()) :: :ok | key_find_error()
  def remove(exvalve, key, option) do
    Exvalve.Server.remove(exvalve, key, option)
  end

  @doc """
  Pushes an item to the queue with the given key. Work can be any 0 arity function
  """
  @spec push(exvalve_ref(), queue_key(), function()) :: :ok
  def push(exvalve, key, work) do
    Exvalve.Server.push(exvalve, key, {work, :erlang.timestamp()})
  end

  @doc """
  Get a list of workers that are currently free
  """
  @spec get_available_workers(exvalve_ref()) :: exvalve_workers()
  def get_available_workers(exvalve) do
    Exvalve.Server.get_available_workers(exvalve)
  end

  @doc """
  gets a count of the workers that are currently free
  """
  @spec get_available_workers_count(exvalve_ref()) :: non_neg_integer()
  def get_available_workers_count(exvalve) do
    :erlang.length(Exvalve.get_available_workers(exvalve))
  end

  @doc """
  Gets the size of the queue with the given key
  """
  @spec get_queue_size(exvalve_ref(), queue_key()) :: non_neg_integer() | key_find_error()
  def get_queue_size(exvalve, key) do
    Exvalve.Server.get_queue_size(exvalve, key)
  end

  @doc """
  Pushes back a set amount of time in order to prevent spamming
  """
  @spec pushback(exvalve_ref(), queue_key()) :: :ok
  def pushback(exvalve, key) do
    Exvalve.Server.pushback(exvalve, key)
  end

  @doc """
  Notifies those listening via GenEvent of various events happening
  inside valvex
  """
  @spec notify(exvalve_ref(), any()) :: :ok
  def notify(exvalve, event) do
    Exvalve.Server.notify(exvalve, event)
  end

  @doc """
  Adds an event handler which can intercept events from exvalve
  and do anything they like with them!
  """
  @spec add_handler(exvalve_ref(), module(), [any()]) :: :ok
  def add_handler(exvalve, mod, args) do
    Exvalve.Server.add_handler(exvalve, mod, args)
  end

  @doc """
  Removes a previously added handler
  """
  @spec remove_handler(exvalve_ref(), module(), [any()]) :: :ok
  def remove_handler(exvalve, mod, args) do
    Exvalve.Server.remove_handler(exvalve, mod, args)
  end

  @doc """
  Updates a key with a new set of queue settings
  """
  @spec update(exvalve_ref(), queue_key(), exvalve_queue()) :: :ok
  def update(exvalve, key, nuq) do
    Exvalve.Server.update(exvalve, key, nuq)
  end
end
