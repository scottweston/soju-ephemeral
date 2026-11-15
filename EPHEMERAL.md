# Ephemeral Mode

A feature branch to implement the following. I want to have a feature to
implement what I am calling "ephemeral mode" which allows the use of `soju` as a
bouncer to smooth out network instability of the users client(s), allow for
multiple clients to share the same IRC account and provide an abstraction
between the users client(s) and irc networks but that will disconnect the user
from all irc networks and await for them to reconnect before restoring irc
connections and channels. Most of this already exists in `soju` this really more
about tightening existing timed functions (such as `message-expiry` and
`disable-inactive-user` which are on timers that are too long to suit this
mode).

The "infinite" (or just large) context of IRC is a burden for some users, they
feel obligated to "read the scrollback" before contributing to their channels.
Some users want the old feel of IRC where presence was part of the experience
and missing some conversations/jokes was all part of the fun and led to more
interesting and real communications. We are seeking to provide this feeling back
to users but keeping the features of a modern bouncer that can keep IRC state
(network auth, channels joined, etc.). A user can "jump back in" get
automagically reattached to servers and channels but without the obligation to
spend the first 15 minutes reading through scrollback before contributing.

## Early Testing

We can approximate the mode via the following existing `soju` options:
```
enable-user-on-auth true
disable-inactive-user 0.1d
message-store db
message-expiry 1d
```

The issues with these options are that `disable-inactive-user` runs on a 4h
timer which does not suit much shorter disconnection time we want for ephemeral
mode. This leaves clients idling in "AFK" for too long. Also the
`message-expiry` timer isn't aligned with the inactive user timer and so the
message store ends up wit remnants of conversations.

## Details

We will keep the features of `soju` with how it remembers irc network, irc
network auth and joined channels but it adds in a more deliberate ephemeral
mode. Once the user disconnects all clients (after some defined hysteresis grace
timer or by commanding it, see below) `soju` will disconnect from all irc
networks (using an optional quit message) and purge the message store.

Suggested example new options:
```
ephemeral-mode true
ephemeral-quit-msg "SIGNAL·DROPPED·▓▒░⠿⠶⠂···"
ephemeral-grace 15m
ephemeral-history 24h
```

We will still use network configured `auto-away` so if the user does briefly
disconnect (maybe changing clients, rebooting, network instability) there
is a signal to other users in the channel that the user isn't connected. We
don't drop upstreams until the grace timer expires (except in cases where
the user commands upstreams to be disconnected via `/quit now`).

In ephemeral mode _purposeful_ upstream disconnection (i.e. triggered by the grace
timer expiring or the user requesting disconnection) triggers a purge of the
`message-store`. If the upstream is disconnected due to an upstream error we
use the normal `soju` upstream re-connection logic. My thinking is we simply use
the existing `disable-inactive-user` code and trigger it when a user requests
disconnect (`/quit now`) or if the grace timer expires before they reconnect.

Ephemeral mode disables the `disable-inactive-user` setting and instead provides
its own logic on how and when to "disable" users. In this sense we are not
_disabing_ the user from the service but instead using the `soju` inactive user
logic to disconnect from upstreams it's more akin to _hibernating_ the user.

As this is a personal fork these settings can be applied server wide to all
users.

## Special Quits

We implement some special `/quit` commands which the user can use to direct
what `soju` does when the client disconnects:

- `/quit now`

Runs the `disable-inactive-user` code on the user and purges messages from the
message-store ready for them to reconnect at a later time with no message
history. It closes all the users downstream clients, `/quit now` is the users
conscience decision to _"log off"_ and they expect to be disconnected from all
upstreams and disconnect all downstreams. If the user has configured their
clients to auto-reconnect then that is their error, we don't need to worry about
that.

We may add more special quits later.

This feature may require the most changes to the `soju` code which currently
shortcircuits `/quit` because by default a bouncer shouldn't quit but instead
just disconnects that downstream. A bare `/quit` will still do just that but we
will be extending `/quit` to allow the user more control of their IRC presence.

## Message Store

The `ephemeral-history` sets the maximum message history window that is stored
(and that can be searched by client) and we always clear/purge any history on
the user being _"disabled"_.

If `ephemeral-mode` is enabled `ephemeral-history` overrides any
`message-expiry` setting, `ephemeral-history` should also support minutes and
hours (e.g. 30m, 4h) values. "History" in this context is for all messages
received by `soju` such as channel messages or private messages. We can
implement this on a suitable periodic cleanup timer. In this case the
`ephemeral-history` will be an approximation +/- the periodic cleanup period,
that's perfectly ok to be +/- multiple minutes, the issue is with the default
timer of 24h which is unworkable for an ephemeral mode.

Purging is only supported for the `db` (or our new `ephemeral` if we
choose to implement it that way) driver of `message-store`. Any other
`message-store` setting with `ephemeral-mode` enabled should cause `soju` to
provide an error message and exit. We are striving to not log persistent
messages when in ephemeral mode and these other drivers seem incompatible with
that. Likewise the `memory` driver seems incompatible as it does not seem to
support purging and instead has a 4K cyclic message log, it is stated as being
"very simple".

If it is cleaner we could implement a new `ephemeral` driver? (I might actually
prefer this solution…). The ephemeral driver would only support sqlite3 databases
(set via the standard `db` configuration option) and provide functions to do the
message purging. `soju` should exit with an error if ephemeral driver is
selected if a non-sqlite3 `db` is configured.

## Startup and Restarts

`soju` should wait for a client to successfully authenticate and connect before
connecting to any upstream networks.

If `soju` crashes or is restarted by an admin (e.g. server reboot etc.) we
should not initiate a purge, instead any messages are left in the store and we
will assume they will purge the next time the user goes through the
connect/disconnect cycle.

## Reconnection scope

Explicitly disabled networks remain disabled but in `ephemeral-mode` disabled
users can always re-enabled on auth. The admin will need to delete users they do
not want on their server. The `ephemeral-mode` is only envisioned for use on
small `soju` instances, nearly exclusively personal single user instances so
dealing with abusive users will not be a problem.

All upstream reconnection logic remains the same.

## Purge Guarantee

This is not intended to be a privacy feature. This feature is to remove the
"burden" or "obligation" to read context when rejoining IRC after an absence to
rekindle the "good old days" of IRC before bouncers. We are using the bouncer to
provide network stability, channel subscriptions, irc auth, etc.

We do not need to worry about data not immediately erasing from underlying
storage or backups, just logically delete the records. If users want to keep
messages it is on them to paste them into a personal "bash.org" instance,
notebook, journal, etc. before disconnecting.

This is the ephemeral feature, messages are ephemeral and vanish after the user
has "logged off" (either by `/quit now` or the grace timer expiring).

The purge is _best effort_ we log but ignore any errors when purging messages if
a race condition between the upstreams and downstreams allow some messages to
not be purged then that is acceptable, this does not need to be over engineered
and perfect.

It may make sense later to provide a `bouncerserv` command to manually trigger a
message purge but we will discover that as we use the feature.

## Network Specific Clients

No change to the underlying `soju` treatment of these sorts of clients. If the
user connects a network specific client the `ephemeral-mode` logic works just as
described.

Like described elsewhere standard ephemeral mode logic dictates that once the
last client disconnects we disconnect from all upstreams (either by grace timer
expiry or `/quit now`), this includes a network specific client if it was the
last client connected. All connected clients are treated the same in this
respect.

## Implementation Details

We will maintain this as a private fork, we should ensure the changes to master
are minimal and all our changes are kept to files we maintain rather than making
changes to existing files that may cause later merge conflicts. Treat it like a
"plugin".

It is understood the code base has no formal plugin support but we just want to
keep our changes to the existing code to as small as possible, ideally just
adding new lines of code not modifying code so as to simplify future rebases.
There will of course be some changes where this is impossible, that's okay as
long as we ensure there is as few of those as possible.

