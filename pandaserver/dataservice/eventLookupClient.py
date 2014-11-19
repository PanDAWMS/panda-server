import urllib, re, string, os, time, commands

# client for eventLookup TAG service
# author:  Marcin.Nowak@cern.ch

class eventLookupClient:

   serverURL = "http://j2eeps.cern.ch/atlas-project-Athenaeum/"
   lookupPage = "EventLookup.jsp"
   getPage = "EventLookupGet.jsp"
   key = "insider"
   workerHost = "atlas-tagservices.cern.ch"
   workerPort = '10004'
   connectionRefusedSleep = 20
   errorPattern = "(Exception)|(Error)|(Lookup cannot be run)|(invalid)|(NOT EXISTING)"

   
   def __init__(self):
      self.output = ""
      self.guids = {}
      self.guidsLine = ""
      self.certProxyFileName = None
      self.certProxy = ""
      self.debug = None
      self.remoteFile = None
      self.rc = 0
      try:
         self.certProxyFileName = os.environ['X509_USER_PROXY']
      except KeyError:
         self.certProxyFileName = '/tmp/x509up_u%s' % os.getuid()
      if not os.access(self.certProxyFileName, os.R_OK):
         print 'EventLookup could not locate user GRID certificate proxy! (do voms-proxy-init)'
         return
      proxy = open(self.certProxyFileName)
      try:
         for line in proxy:
            self.certProxy += line
      finally:
         proxy.close()

         
   def workerURL(self):
      if self.workerHost.find(":") > 0:
         # port number together with the host name, possibly from commandline option         
         return "http://" + self.workerHost
      else:
         return "http://" + self.workerHost + ":" + self.workerPort


   def getCurlCmd(self):
      try:
         return self.curlCmd         
      except AttributeError:
         self.curlCmd = 'curl --compressed -s -S -E ' + self.certProxyFileName
         try:
            self.certDir = os.environ['X509_CERT_DIR']
         except KeyError:
            self.certDir = '/etc/grid-security/certificates'
         rc, response = commands.getstatusoutput( 'uname -r' )
         if 'el6' in response.split('.'):
            if self.debug:  print "detected SLC6 for curl"
            self.curlCmd += ' --capath ' + self.certDir + ' --cacert ' + self.certProxyFileName
         else:
            self.curlCmd += ' -k '
         return self.curlCmd         

 
   def doLookup(self, inputEvents, async=None, stream="", tokens="",
                amitag="", extract=False):
      """ contact the server and return a list of GUIDs
      inputEvents  - list of run-event pairs
      async - request query procesing in a separate process, client will poll for results
      stream - stream
      tokens - token names
      amitag - used to select reprocessing pass (default empty means the latest)
      """
      if inputEvents == []:
         return []

      runs_events = ""
      runs = set()
      sep = ""
      for run_ev in inputEvents:
         runs_events += sep + run_ev[0] + " " + run_ev[1]
         sep = "\n"
         runs.add(run_ev[0]);

      if async is None:
         if len(runs) > 50 or len(inputEvents) > 1000:
            async = True
      if async:
         asyncStr = "true"
      else:
         asyncStr = "false"

      query_args = { 'key': self.key,
                     'worker': self.workerURL(),
                     'runs_events': runs_events,
                     'cert_proxy': self.certProxy,
                     'async': asyncStr,
                     'stream': stream,
                     'amitag': amitag,
                     'tokens': tokens
                     }
      if extract:
         query_args['extract'] = "true"

      self.talkToServer(self.serverURL + self.lookupPage, query_args)
      if not async:
         for line in self.output:
            if re.search("502 Bad Gateway", line):
               # usually signifies a timeout on the J2EE server
               print "Timeout detected. Retrying in asynchronous mode"
               query_args['async'] = "true"
               self.talkToServer(self.serverURL + self.lookupPage, query_args)
               break

      self.remoteFile = None
      for line in self.output:
         m = re.search("FILE=(.+)$", line)
         if m:
            return self.waitForFile( m.group(1) )         

      return self.scanOutputForGuids()
   

   def talkToServer(self, url, args):
      encoded_args = urllib.urlencode(args)
      if self.debug:
         print "Contacting URL: " + url
         print encoded_args

      for _try in range(1,6):
         response = urllib.urlopen(url, encoded_args)
         self.output = []
         retry = False
         for line in response:
            self.output.append(line)
            if re.search("Connection refused", line):
               retry = True
         if retry:
            if self.debug:
               print "Failed to connect to the server, try " + str(_try)
            time.sleep(self.connectionRefusedSleep)
         else:
            break

         
   def doLookupSSL(self, inputEvents, stream="", tokens="", amitag="", mcarlo = False, extract=False):
      """ contact the server using SSL and return a list of GUIDs
      inputEvents  - list of run-event pairs OR a filename (file contains run+event pair per line)
      stream - physics stream
      tokens - token names for GUIDs to retrieve
      amitag - used to select reprocessing pass (default: empty which means the latest pass)
      mcarlo - if True ask for MC TAGs only
      """

      if isinstance(inputEvents, basestring):
         #events from a file
         runs_events = "<" + inputEvents
      else:
         if inputEvents == []:
            return []
         runs_events = ""
         sep = ""
         for run_ev in inputEvents:
            runs_events += sep + run_ev[0] + " " + run_ev[1]
            sep = "\n"

      tagtype = 'TAG'
      args = ' -F "runs_events=' + runs_events +'"'
      if tokens != "": args = args + ' --form-string "tokens=' + tokens +'"'
      if stream != "": args = args + ' --form-string "stream=' + stream +'"'
      if amitag != "": args = args + ' --form-string "amitag=' + amitag +'"'
      if mcarlo:       args = args + ' -F tagtype=TAG_MC'
      if extract:      args = args + ' -F extract=yes'

      if self.talkToServerSSL("eventLookup", args):
         return None
      return self.scanOutputForGuids()
   

   def talkToServerSSL(self, service, args):
      cmd = self.getCurlCmd() + " https://"+self.workerHost+"/tagservices/EventLookup/www/" + service
      cmd += ' -F "certificate=<' + self.certProxyFileName + '"'
      cmd += args
      
      if self.debug:
         print "Executing command: " + cmd

      for _try in range(1,6):
         self.rc, response = commands.getstatusoutput( cmd )
         self.output = []
         retry = False
         for line in response.split('\n'):
            self.output.append(line)
            if re.search("Connection refused", line):
               retry = True
         if retry:
            if self.debug:
               print "Failed to connect to the server, try " + str(_try)
            time.sleep(self.connectionRefusedSleep)
         else:
            break
      return self.rc

 
   def scanOutputForGuids(self):
      """ Scan the server output looking for a line with GUIDs
      return list of GUIDs if line found, put GUIDs in self.guids
      return None in case of errors
      """
      self.guids = {}
      self.tags = []
      self.tagAttributes = None
      stage = None
      tokpat = re.compile(r'[[]DB=(?P<FID>.*?)[]]')
      for line in self.output:
         if re.search(self.errorPattern, line, re.I):
            #print " -- Error line matched: " + line
            return None
         if stage == "readTags":
            if line[0:1] == ":":
               # break the line up into attributes, extract GUIDs
               values = []
               for attr in string.split(line[1:]):
                  tok = tokpat.match(attr)
                  if tok:
                     attr = tok.group('FID')
                     # self.guids - TODO - populate the guids dict
                  values.append(attr)
               self.tags.append( values )
               continue
            else:
               return (self.tagAttributes, self.tags)
         if re.match("\{.*\}$", line):
            guids = eval(line)
            if type(guids).__name__!='dict':
               return None
            self.guids = guids
            return guids
         if re.search("TAGs extracted:", line):
            stage = "readAttribs"
            continue
         if stage == "readAttribs":
            self.tagAttributes = string.split(line.strip(),",")
            stage = "readTags"
            continue
      return None


   def checkError(self, output=None):
      """ return code if known error found, else None. Prints error message
      """
      error1 = "You may try selectuing a different worker host. Use debug option to see the entire output"
      # check for errors
      if not output:  output = self.output
      if type(output) == type('str'):  output = output.split('\n')
      for line in output:
         if re.search("certificate expired", line):
            print "Your CA certificate proxy may have expired. The returned error is:\n" + line
            return 2
         if re.search("SSL connect error", line):
            print line
            checkcmd = 'voms-proxy-info -exists -file '+self.certProxyFileName
            rc, out = commands.getstatusoutput(checkcmd)
            if rc==0:
               return 20  # reason not known
            if rc==1:
               print "Certificate Proxy is NOT valid. Check with " + checkcmd
               return 21
            print "Check if your Certificate Proxy is still valid: " + checkcmd
            return 25
         if re.search("unable to use client certificate", line):
            print line
            return 22
         if self.remoteFile and re.match("NOT EXISTING", line):
            print "File '" + self.remoteFile + "' not found on " + self.workerHost
            return 3
         if( re.search("AthenaeumException: No response from server", line)
             or re.search("ConnectException: Connection refused", line) ):
            print "ERROR contacting " + self.workerHost
            print error1
            return 4
         if re.search("AthenaeumException: Can't execute commad", line):
            print "ERROR processing request on " + self.workerHost
            print error1
            return 5
      return None
      

   def waitForFile(self, file):
      """ Wait for the server to do EventLookup and store results in file <file>
      Retrieve the file and scan for GUIDs - return them if found
      """
      query_args = { 'key': self.key,
                     'worker': self.workerURL(),
                     'file' : file,
                     'wait_time' : "45"
                     }
      self.remoteFile = file
      if self.debug:
         print "EventLookup waiting for server.  Remote file=" + file

      ready = False  
      while not ready:
         self.talkToServer(self.serverURL + self.getPage, query_args)
         ready = True
         for line in self.output:
            if re.match("NOT READY", line):
               if self.debug:
                  print "received NOT READY"
               time.sleep(1)
               ready = False

      return self.scanOutputForGuids()
 
