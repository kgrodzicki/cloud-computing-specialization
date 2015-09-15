#! /usr/bin/env python -u
# coding=utf-8

__author__ = 'Sayed Hadi Hashemi'

import json
import urllib
import urllib2
import hashlib
import email
import email.message
import email.encoders


class CourseraSubmission(object):
    def __init__(self, **kwargs):
        self.email = ""
        self.password = ""
        self.course_id = ""
        self.part_ids = []
        self.part_names = []

        self.__dict__.update(kwargs)

    def _login_prompt(self):
        """Prompt the user for login credentials. Returns a tuple (login, password)."""
        (login, password) = self._basic_prompt()
        return login, password

    @staticmethod
    def _basic_prompt():
        """Prompt the user for login credentials. Returns a tuple (login, password)."""
        login = raw_input('Login (Email address): ')
        password = raw_input('One-time Password (from the assignment page. This is NOT your own account\'s password): ')
        return login, password

    def _auth_get_challenge(self, sid):
        """Gets the challenge salt from the server. Returns (email,ch,state,ch_aux)."""
        url = self._auth_get_challenge_url()
        values = {'email_address': self.email, 'assignment_part_sid': sid, 'response_encoding': 'delim'}
        data = urllib.urlencode(values)
        req = urllib2.Request(url, data)
        response = urllib2.urlopen(req)
        text = response.read().strip()

        # text is of the form email|ch|signature
        splits = text.split('|')
        if len(splits) != 9:
            print 'Badly formatted challenge response: %s' % text
            return None
        return splits[2], splits[4], splits[6], splits[8]

    def _auth_challenge_response(self, challenge):
        sha1 = hashlib.sha1()
        sha1.update("".join([challenge, self.password]))  # hash the first elements
        digest = sha1.hexdigest()
        str_answer = ''
        for i in range(0, len(digest)):
            str_answer = str_answer + digest[i]
        return str_answer

    def _auth_get_challenge_url(self):
        """Returns the challenge url."""
        return "https://class.coursera.org/" + self.course_id + "/assignment/challenge"

    def _get_submission_url(self):
        """Returns the submission url."""
        return "https://class.coursera.org/" + self.course_id + "/assignment/submit"

    def _submit_solution(self, ch_resp, sid, output, source, state, ch_aux):
        """Submits a solution to the server. Returns (result, string)."""
        source_64_msg = email.message.Message()
        source_64_msg.set_payload(source)
        email.encoders.encode_base64(source_64_msg)

        output_64_msg = email.message.Message()
        output_64_msg.set_payload(output)
        email.encoders.encode_base64(output_64_msg)
        values = {'assignment_part_sid': sid,
                  'email_address': self.email,
                  'submission': output_64_msg.get_payload(),
                  'submission_aux': source_64_msg.get_payload(),
                  'challenge_response': ch_resp,
                  'state': state
                  }
        url = self._get_submission_url()
        data = urllib.urlencode(values)
        req = urllib2.Request(url, data)
        response = urllib2.urlopen(req)
        string = response.read().strip()
        result = 0
        return result, string

    @staticmethod
    def get_file_content(file_name):
        with open(file_name, "r") as fp:
            return fp.read()

    def submit(self):
        print '\n== Connecting to Coursera ... '
        for part_index, part_id in enumerate(self.part_ids):
            ret = self._auth_get_challenge(part_id)
            if not ret:
                print '\n!! Error: %s\n' % self.email
                return False

            (login, ch, state, ch_aux) = ret
            if (not self.email) or (not ch) or (not state):
                print '\n!! Error: %s\n' % self.email
                return

            ch_resp = self._auth_challenge_response(ch)
            (result, string) = self._submit_solution(ch_resp, part_id, self.output(part_index), self.aux(part_index),
                                                     state, ch_aux)

            print '== (%s) %s' % (self.part_names[part_index], string.strip())
            if "We could not verify your username / password" in string:
                return False
        return True

    def aux(self, part_index):
        return json.dumps({})

    def output(self, part_index):
        pass

    def run(self):
        pass

    def init(self):
        print '==\n== [sandbox] Submitting Solutions \n=='

        (self.email, self.password) = self._login_prompt()
        if not self.email:
            print '!! Submission Cancelled'
            return False

        if len(self.part_ids) > 0:
            sid = self.part_ids[0]
            ret = self._auth_get_challenge(sid)
            if not ret:
                print '\n!! Error: %s\n' % self.email
                return False

            (login, ch, state, ch_aux) = ret
            if (not login) or (not ch) or (not state):
                print '\n!! Error: %s\n' % login
                return False
        else:
            return False
        return True

    def make_sumbission(self):
        if self.init():
            self.run()
            while not self.submit():
                ret = raw_input('Try Again? (Y/N)')
                if len(ret.strip()) > 0 and ret.strip().lower()[0] == 'y':
                    self.password = raw_input(
                        'One-time Password (from the assignment page. This is NOT your own account\'s password): ')
                else:
                    break
