from django.db import models
from django.contrib import admin
from talent_evaluation.settings import BASE_DIR
# Create your models here.

import os
import sys

class Evaluation(models.Model):
    name = models.CharField(max_length=300)
    date_created = models.DateTimeField(auto_now_add=True)

    def save(self, *args, **kwargs):
        command = "python " + BASE_DIR + os.sep + 'bills_file.py'
        print(command)
        #os.system()
        super(Evaluation, self).save(*args, **kwargs)

    def __str__(self):
        return self.name




admin.site.register(Evaluation)