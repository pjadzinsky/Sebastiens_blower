""" Default console progress-bars for monitoring long-running operations. """

from common.log import *
from progressbar import Bar, Percentage, ETA, ProgressBar

def create_progress_bar(num_items):
  widgets = [Percentage(), ' ', Bar(), ' ', ETA()]  
  pbar = ProgressBar(widgets=widgets, maxval=num_items).start()
  return pbar
  
  


