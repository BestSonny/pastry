defmodule PSTTest do
  use ExUnit.Case
  doctest PST

  test "greets the world" do
    assert PST.hello() == :world
  end
end
