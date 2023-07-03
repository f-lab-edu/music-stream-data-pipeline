# Eventsim
Eventsim is a software program that creates synthetic event data for the purpose of testing and demonstrations. It is specifically designed to simulate user interactions with a fictional music website, similar to platforms like Spotify. The generated data closely resembles real user activity, but it is entirely fictional and not based on actual user behavior. Original repo can be found in [here](https://github.com/Interana/eventsim). And [streamify](https://github.com/ankurchavda/streamify/tree/main/eventsim) was also refered.

# Running

#### Build
```bash
docker build -t events:1.0 .
```
#### Run With Kafka Configured On Localhost
```bash
docker run -it \
  --network host \
  events:1.0 \
    -c "examples/example-config.json" \
    --start-time "`date +"%Y-%m-%dT%H:%M:%S"`" \
    --end-time "2022-03-18T17:00:00" --nusers 20000 \
    --kafkaBrokerList localhost:9092 \
    --continuous
```

# Data Explanation

**Eventsim** collects logs from a fake music website, and the following is a data explanation derived from it.

## 1. auth_events

This data is a collection of events related to user authentication. Each event represents a user authentication action that occurred at a specific timestamp (ts):

- `ts`: The timestamp of the authentication event in milliseconds since a specific reference point (e.g., January 1, 1970).
- `sessionId`: A unique identifier for the session during which the authentication event occurred.
- `level`: The level of the user account, indicating whether it is a paid or free account.
- `itemInSession`: The index of the authentication event within the session.
- `city`: The city where the authentication event occurred.
- `zip`: The zip code of the location where the authentication event occurred.
- `state`: The state where the authentication event occurred.
- `userAgent`: The user agent string, representing the web browser or client used for the authentication.
- `lon`: The longitude coordinate of the location where the authentication event occurred.
- `lat`: The latitude coordinate of the location where the authentication event occurred.
- `userId`: A unique identifier for the user associated with the authentication event.
- `lastName`: The last name of the user.
- `firstName`: The first name of the user.
- `gender`: The gender of the user.
- `registration`: The timestamp when the user registered their account in milliseconds since a specific reference point.
- `tag`: A tag associated with the authentication event.
- `success`: A boolean value indicating whether the authentication was successful (true) or not (false).

---

## 2. listen_events

This data represents a series of events related to music listening:

- `artist`: The name of the artist who performed the song.
- `song`: The title of the song.
- `duration`: The duration of the song in seconds.
- `ts`: The timestamp of the event, representing the time when the song was played. The timestamp is in Unix time format, which is the number of milliseconds since January 1, 1970.
- `sessionId`: A unique identifier for the user session. Sessions group together a series of related events or interactions.
- `auth`: Indicates whether the user was logged in or not at the time of the event.
- `level`: The level of the user's subscription (e.g., free, paid).
- `itemInSession`: The index of the item within the user's session. It represents the position of the song in the user's playlist or queue.
- `city`: The city where the event occurred.
- `zip`: The ZIP code of the event location.
- `state`: The state where the event occurred.
- `userAgent`: Information about the user's web browser or user agent.
- `lon`: The longitude coordinate of the event location.
- `lat`: The latitude coordinate of the event location.
- `userId`: A unique identifier for the user.
- `lastName`: Specifies the last name of the user.
- `firstName`: Specifies the first name of the user.
- `gender`: The gender of the user.
- `registration`: The timestamp of the user's registration.
- `tag`: A tag associated with the event (e.g., "control").

---

## 3. page_view_events

This data represents user interactions with a web page:

- `ts`: The timestamp of the event, represented in milliseconds since a particular reference point (e.g., Unix epoch).
- `sessionId`: A unique identifier for the user session during which the event occurred.
- `page`: The type of page or action associated with the event (e.g., "NextSong" indicates that the user moved to the next song).
- `auth`: The authentication status of the user ("Logged In" or "Logged Out").
- `method`: The HTTP method used for the event (e.g., "PUT" represents an update or modification).
- `status`: The HTTP status code returned for the event (e.g., 200 indicates a successful request).
  level: The level of the user's account (e.g., "free" or "premium").
- `itemInSession`: The index of the item within the user's session.
- `city`: The city where the event occurred.
- `zip`: The ZIP code of the location where the event occurred.
- `state`: The state where the event occurred.
- `userAgent`: Information about the user's web browser and operating system.
- `lon`: The longitude coordinate of the event location.
- `lat`: The latitude coordinate of the event location.
- `userId`: A unique identifier for the user.
- `lastName`: The last name of the user.
- `firstName`: The first name of the user.
- `gender`: The gender of the user.
- `registration`: The timestamp when the user registered, represented in milliseconds.
- `tag`: Additional information or metadata associated with the event.
- `artist`: The name of the artist associated with the event (e.g., the artist of the song being played).
- `song`: The name of the song associated with the event.
- `duration`: The duration of the song in seconds.

---

## 4. status_change_events

This data represents a collection of status_change_events from the Eventsim log:

- `ts`: Represents the timestamp of the event in milliseconds since a specific reference point (e.g., Unix epoch time).

- `sessionId`: A unique identifier for the user session during which the event occurred.

- `auth`: Indicates the authentication status of the user during the event (e.g., "Logged In" or "Logged Out").

- `level`: Represents the user's subscription level (e.g., "free" or "premium").

- `itemInSession`: Denotes the sequential number of the item within the user's session.

- `city`: Specifies the city associated with the user's location.

- `zip`: Represents the zip code associated with the user's location.

- `state`: Indicates the state or province associated with the user's location.

- `userAgent`: Provides information about the user's web browser and operating system.

- `lon`: Represents the longitude coordinate of the user's location.

- `lat`: Represents the latitude coordinate of the user's location.

- `userId`: Indicates the unique identifier for the user.
- `lastName`: Specifies the last name of the user.

- `firstName`: Specifies the first name of the user.

- `gender`: Represents the gender of the user.

- `registration`: Indicates the timestamp of the user's registration in milliseconds.

- `tag`: Represents a control tag associated with the event.
