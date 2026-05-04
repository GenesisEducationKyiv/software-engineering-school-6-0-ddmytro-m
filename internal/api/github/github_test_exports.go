//go:build testing

package github

// it is only available when compiled with the 'testing' build tag.
func (c *Client) SetRateLimitsForTest(limits RateLimits) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastLimits = limits
}
