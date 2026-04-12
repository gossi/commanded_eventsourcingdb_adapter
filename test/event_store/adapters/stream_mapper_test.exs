defmodule Commanded.EventStore.Adapters.EventSourcingDB.InfraTest do
  alias Commanded.EventStore.Adapters.EventSourcingDB.StreamMapper

  use ExUnit.Case, async: true

  test "to_subject" do
    assert "/" == StreamMapper.to_subject("", "")
    assert "/" == StreamMapper.to_subject("", "/")
    assert "/" == StreamMapper.to_subject("/", "/")
    assert "/" == StreamMapper.to_subject("/", "")
    assert "/" == StreamMapper.to_subject("", :all)
    assert "/" == StreamMapper.to_subject("/", :all)
    assert "/lala" == StreamMapper.to_subject("", "lala")
    assert "/lala" == StreamMapper.to_subject("/", "lala")
    assert "/lala" == StreamMapper.to_subject("", "lala/")
    assert "/lala" == StreamMapper.to_subject("/", "lala/")
  end
end
