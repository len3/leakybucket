from datetime import datetime
from time import sleep
import math

class LeakyBucketThrottler(object):
	"""Leaky Bucket Throttler"""
	def __init__(self, bucket_capacity_items, restore_qty, restore_rate_in_seconds):
		super(LeakyBucketThrottler, self).__init__()
		self.bucket_capacity_items = bucket_capacity_items
		self.restore_qty = int(math.floor(restore_qty))
		self.restore_rate_in_seconds = float(restore_rate_in_seconds)
		self.bucket_current_items = 0
		self.init_time = datetime.utcnow()
		self.last_leak_time = self.init_time
		self.remainder = 0


	def seconds_elapsed_since(self, start_datetime):
		current_time = datetime.utcnow()
		return (current_time - start_datetime).total_seconds()


	def update_bucket(self):
		elapsed_time = self.seconds_elapsed_since(self.last_leak_time)
		leakage = (elapsed_time * self.restore_qty/self.restore_rate_in_seconds) + self.remainder
		self.remainder = leakage - math.floor(leakage)
		self.bucket_current_items = max(0, self.bucket_current_items - int(leakage))
		self.last_leak_time = datetime.utcnow()
		

	def wait_for_capacity(self):
		while self.bucket_current_items >= self.bucket_capacity_items:
			# calculate time until next free slot
			time_to_wait = self.restore_rate_in_seconds - self.seconds_elapsed_since(self.last_leak_time)
			if time_to_wait > 0:
				sleep(time_to_wait)
			self.update_bucket()
			

	def execute_when_ready(self, f, **kwargs):
		self.update_bucket()
		self.wait_for_capacity()
		self.bucket_current_items = self.bucket_current_items + 1
		# self.print_status()
		# self.last_leak_time = datetime.utcnow()
		return f(self=f, **kwargs)


	def print_status(self):
		self.update_bucket()
		print "Bucket capacity: %s" % self.bucket_capacity_items
		print "Bucket leaks %s items every %s seconds\n"  % ( self.restore_qty, self.restore_rate_in_seconds)
		print "Currently holding %s items" % self.bucket_current_items
		if self.bucket_current_items < self.bucket_capacity_items:
			print "Bucket has capacity now!"  
		else:
			print "Next slot will be open in %s seconds" % (self.restore_rate_in_seconds - self.seconds_elapsed_since(self.last_leak_time))


