"""
In our project Zipline, https://github.com/quantopian/zipline/, we consume time series data one bar at a time. We provide support for calculations that are performed over a fixed length, e.g. over the last 15 days of data. The data which those calculations are performed over are updated with the most recent bar as it comes in.

Instead of creating a fresh Panel on each bar and reindexing all of the data into the underlying data storage, @wesm helped us with a RollingPanel object, https://github.com/quantopian/zipline/blob/2492feb938c21f7577e6c05fa8cd7bfd57863d25/zipline/utils/data.py

The RollingPanel is working well for our purposes, and we'd like to upstream it to make the behavior available to all users of pandas.

There are a couple idionsyncratic to our codebase parameters that would need to be generalized.
e.g.:

the use sids would need to be a more general minor_axis parameter.
the dtype of the index buffer would also need to be more general, where the version in Zipline is tuned/hard code for being a pd.DatetimeIndex in UTC.
@wesm mentioned that instead of a rolling version pd.Panel it may make more sense to start a rolling version of a NDFrame.

(And then I assume have a RollingPanel, RollingSeries, etc. that use RollingNDFrame instead of NDFrame, though that level of the API design I am not sure I am qualified to speak to as of yet.)

attn: @jreback, I believe this is under your purview?

If the idea passes muster, what would be a good next step for getting the rolling behavior into pandas?
(Or has there been work on a RollingNDFrame started already, that I could help pitch in on?)
"""

"""
Here's an edited example from our batch_transform module.

The RollingPanel is initialized as such[1], where:

self.window_length: Number of days to perform the calculation over.
self.field_names: The open, high, low, close, price, etc. fields being captured by the panel.
sids, The identifiers for the stocks being captured by the panel.
self.daily_rolling_panel = RollingPanel(
    self.window_length,
    self.field_names,
    sids)
The data is then updated with the following code[2], where:

event.dt: Is the current active historical time in the backtest.
event.data: A dictionary of the current bar's open, high, low, close data. (i.e. the same values as field_names.)
self.field_names and sids: Are the same values as the panel is in initialization, though population of sids can change throughout the backtest.
# Store event in rolling frame
self.rolling_panel.add_frame(
    event.dt,
    pd.DataFrame(
        event.data,
        index=self.field_names,
        columns=sids))
[1] https://github.com/quantopian/zipline/blob/2492feb938c21f7577e6c05fa8cd7bfd57863d25/zipline/transforms/batch_transform.py#L265
[2]
https://github.com/quantopian/zipline/blob/2492feb938c21f7577e6c05fa8cd7bfd57863d25/zipline/transforms/batch_transform.py#L293
"""