defmodule Exvalve.Mixfile do
  use Mix.Project

  def project do
    [app: :exvalve,
     version: "1.0.4",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [
      mod: {Exvalve.Application, []},
      applications: [:kernel, :stdlib, :logger, :gun, :jsx, :cowboy]
    ]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:cowboy, git: "https://github.com/ninenines/cowboy.git", branch: "master"},
      {:cowlib, git: "https://github.com/ninenines/cowlib.git", branch: "master", override: true},
      {:ranch,  git: "https://github.com/extend/ranch.git", branch: "master", override: true},
      {:gun,    git: "https://github.com/ninenines/gun.git", branch: "master"},
      {:jsx, "2.8.0"}
    ]
  end
end
