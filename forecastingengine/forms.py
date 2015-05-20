__author__ = 'root'
from django import forms
from django.core.validators import EMPTY_VALUES
from django.utils.encoding import smart_text

class UploadFileForm(forms.Form):
    startdate = forms.DateField(required=True)
    file = forms.FileField(required=True)
