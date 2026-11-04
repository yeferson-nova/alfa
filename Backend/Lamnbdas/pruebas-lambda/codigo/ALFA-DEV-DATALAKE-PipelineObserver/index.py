import json,os,time,boto3
G=boto3.client('glue');S=boto3.client('stepfunctions');N=boto3.client('sns')
E=os.environ;V=E['ENV'];R=E['AWS_REGION'];D='-'
def out(r,m=None):
 r['Environment']=V
 if m:r.update(m);r['_aws']={'Timestamp':int(time.time()*1000),'CloudWatchMetrics':[{'Namespace':E['NS'],'Dimensions':[['Environment']],'Metrics':[{'Name':k,'Unit':'Seconds' if 'Dur' in k else 'Count'} for k in m]}]}
 print(json.dumps(r,default=str))
def job(d):
 s=d.get('state');n=d['jobName'];r={'recordType':'GLUE_JOB','job':n,'runId':d.get('jobRunId',D),'state':s}
 if d.get('notificationCondition'):r.update(recordType='GLUE_JOB_DELAYED',message=d.get('message',D));return out(r,{'GlueJobDelayed':1})
 m={'GlueJobSucceeded':int(s=='SUCCEEDED'),'GlueJobFailed':int(s in('FAILED','TIMEOUT','ERROR'))}
 try:
  j=G.get_job_run(JobName=n,RunId=d['jobRunId'])['JobRun'];t=j.get('ExecutionTime',0)
  r.update(durationSec=t,dpuHours=round((j.get('DPUSeconds') or t*(j.get('MaxCapacity') or 0))/3600,3),attempt=j.get('Attempt',0),error=j.get('ErrorMessage',D));m['GlueJobDurationSeconds']=t
 except Exception:r.update(error=d.get('message',D))
 out(r,m)
def crawler(d):
 s=d.get('state');t=d.get('runningTime (sec)');r={'recordType':'GLUE_CRAWLER','crawler':d['crawlerName'],'state':s,'error':d.get('errorMessage',D)}
 for k in('tablesCreated','tablesUpdated','partitionsCreated'):r[k]=d.get(k,'0')
 m={'CrawlerSucceeded':int(s=='Succeeded'),'CrawlerFailed':int(s=='Failed')}
 if t:r['durationSec']=m['CrawlerDurationSeconds']=float(t)
 out(r,m)
def sfn(d):
 a=d['executionArn'];sm=d['stateMachineArn'].split(':')[-1];st=d['status'];ev=[];kw={'executionArn':a,'maxResults':1000}
 for _ in range(10):
  p=S.get_execution_history(**kw);ev+=p['events']
  if 'nextToken' not in p:break
  kw['nextToken']=p['nextToken']
 by={e['id']:e for e in ev}
 def own(e):
  i=e.get('previousEventId')
  while i in by:
   x=by[i]
   if x['type'].endswith('StateEntered'):return x['stateEnteredEventDetails']['name']
   i=x.get('previousEventId')
  return D
 b={'stateMachine':sm,'execution':d['name'],'executionStatus':st};op={};tg={};ec={};er=[];sp=[]
 for e in ev:
  t=e['type'];ts=e['timestamp']
  if t.endswith('StateEntered'):op.setdefault(e['stateEnteredEventDetails']['name'],[]).append((t[:-12],ts))
  elif t.endswith('StateExited'):
   n=e['stateExitedEventDetails']['name']
   if op.get(n):y,s=op[n].pop(0);sp.append((n,y,(ts-s).total_seconds(),1))
  elif t=='TaskScheduled':
   x=e['taskScheduledEventDetails']
   try:p=json.loads(x.get('parameters') or '{}')
   except Exception:p={}
   tg[own(e)]=(x['resourceType']+':'+x['resource'],p.get('JobName') or p.get('Name') or p.get('FunctionName') or D)
  elif t.endswith(('Failed','TimedOut','Aborted')):
   x=next((v for k,v in e.items() if k.endswith('EventDetails')),{});n='EXECUTION' if t.startswith('Execution') else own(e);ec[n]=ec.get(n,0)+1
   r=dict(b,recordType='SFN_STEP_ERROR',step=n,eventType=t,error=x.get('error',D),cause=(x.get('cause') or D)[:1000]);er.append(r);out(r)
 for n,l in op.items():
  for y,s in l:sp.append((n,y,(ev[-1]['timestamp']-s).total_seconds(),0))
 for n,y,u,ok in sp:
  rs,tt=tg.get(n,(D,D));c=ec.get(n,0)
  if y in('Choice','Pass','Succeed','Fail') and not c:continue
  out(dict(b,recordType='SFN_STEP',step=n,stepType=y,durationSec=u,resource=rs,target=tt,errors=c,status=('WITH_ERRORS' if c else 'SUCCEEDED') if ok else st))
 u=(d['stopDate']-d['startDate'])/1000 if d.get('stopDate') else 0
 out(dict(b,recordType='SFN_EXECUTION',durationSec=u,failedSteps=','.join(dict.fromkeys(r['step'] for r in er)) or D))
 if st!='SUCCEEDED':
  g=[f'Estado: {st}',f'Duracion: {u:.0f}s',f'https://{R}.console.aws.amazon.com/states/home?region={R}#/v2/executions/details/{a}','','Errores:']
  for r in er[:10]:rs,tt=tg.get(r['step'],(D,D));g+=[f"- {r['step']} | {rs} | {tt}",f"  {r['eventType']}: {r['error']}",'  '+r['cause'][:500]]
  N.publish(TopicArn=E['WARN' if st=='ABORTED' else 'CRIT'],Subject=f'[{V}] SFN {st}: {sm}'[:99],Message='\n'.join(g))
def handler(ev,ctx):
 t=ev['detail-type'];d=ev['detail']
 if 'Job' in t:job(d)
 elif 'Crawler' in t:crawler(d)
 else:sfn(d)
