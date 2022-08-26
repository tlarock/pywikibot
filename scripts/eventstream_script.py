import datetime
from pywikibot.comms.eventstreams import EventStreams

ts_format = "%Y-%m-%dT%H:%M:%SZ"

# Start time for stream events
since = "2022-07-27T00:00:00Z"
# Time to stop streaming events
endtime = "2022-07-29T00:00:00Z"

# endtime a datetime object
end_ts = datetime.datetime.strptime(endtime, ts_format)

# Output filename
output_file = "../wiki-eventstreaming/data/csv/events_since-" + since + "_end-" + endtime + ".csv"

# Open the stream
# NOTE: When I surround the streaming in a try/except block it hangs. Not
# handling any errors myself.
stream = EventStreams(streams='page-create,page-delete,page-links-change',
                      since=since)

# Filter to english Wikipedia
# NOTE: This filter doesn't seem to be enough. Filtering for en.wikipedia
# below instead.
stream.register_filter(database="enwiki")

# $schema --> Boolean (have we seen an event for this schema past the timestamp?)
# NOTE: We still read from all streams, even if we are done with one. So we are
# hitting the server more than we need. Might be good to handle each
# separately.
break_loop = {key:False for key in ["ADDED", "DELETED", "LINK"]}

# Track progress
events_streamed = 0
print_interval = 10_000
print_counter = 0
all_changes = []
# Open the output file for writing
with open(output_file, 'w') as fout:
    while True:
        # Stream the next change
        # Returns a dictionary of the event/JSON object
        change = next(iter(stream))

        # Filter out non-article pages and redirects
        if change["page_namespace"] != 0 or change["page_is_redirect"]:
            continue

        # Filter out non-english wikipedia pages
        # NOTE: This should be happening based on the filter, but I have found
        # it is not consistent and so I am just doing it this way
        if "en.wikipedia" not in change["meta"]["uri"]:
            continue

        # NOTE: Temporarily saving output
        all_changes.append(change)

        # Get the event type from $schema
        if "create" in change["$schema"]:
            event_type = "ADDED"
        elif "delete" in change["$schema"]:
            event_type = "DELETED"
        elif "links-change" in change["$schema"]:
            event_type = "LINK"

        # Halting condition
        curr_ts = datetime.datetime.strptime(change["meta"]["dt"], ts_format)
        if curr_ts > end_ts:
            # We have reached the endtime for this stream
            break_loop[event_type] = True
            # If we reach endtime for all streams, break the loop
            if all(break_loop.values()):
                print("Reached end timestamp: " + change["meta"]["dt"])
                break
            # Otherwise, keep streaming
            continue

        # Get the info we keep for every event
        basic_info = [change["page_title"],
                            change["page_id"],
                            change["rev_id"],
                            change["meta"]["dt"]]

        # Process + write the change depending on event_type
        if event_type == "ADDED" or event_type == "DELETED":
            basic_info += ["null"]
            fout.write("\t".join(map(str, [event_type] + basic_info)) + "\n")
        elif event_type == "LINK":
            if "added_links" in change:
                for link_info in change["added_links"]:
                    if not link_info["external"]:
                        link = "/".join(link_info["link"].split("/")[2:])
                        fout.write("\t".join(map(str, ["LINK-ADD"] + basic_info +
                                            [link])) + "\n")
            if "removed_links" in change:
                for link_info in change["removed_links"]:
                    if not link_info["external"]:
                        link = "/".join(link_info["link"].split("/")[2:])
                        fout.write("\t".join(map(str, ["LINK-REMOVE"] + basic_info +
                                            [link])) + "\n")

        # Print progress
        print_counter += 1
        events_streamed += 1
        if print_counter == print_interval:
            print(datetime.datetime.now(), " Streamed: ", events_streamed)
            print_counter = 0

import pickle
with open("../wiki-eventstreaming/data/pickles/events_since-" + since + "_end-" + endtime + ".pickle", "wb") as fout:
    pickle.dump(all_changes, fout)
