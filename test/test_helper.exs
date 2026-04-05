Testcontainers.start_link()

ExUnit.start()

# ExUnit.configure(
#   exclude: [
#     test: ~r/should fail to append to a stream because of wrong expected version$/
#   ]
# )

# names_to_exclude = ["should fail to append to a stream because of wrong expected version"]

# ExUnit.configure(
#   exclude: [:test],
#   include: Enum.map(names_to_exclude, &{:test, &1})
# )
