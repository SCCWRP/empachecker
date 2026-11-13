const fs=require('fs'),path=require('path'),vm=require('vm'),assert=require('assert/strict');
const html=fs.readFileSync(path.resolve(__dirname,'../../proj/templates/logger_review_signin.html'),'utf8');
function boot(response) {
  const ids={};
  for(const [,id] of html.matchAll(/id="([^"]+)"/g)) ids[id]={hidden:false,value:'',disabled:false,textContent:'',focus(){},classList:{toggle(){}}};
  const state={sent:[],reloads:0};
  const context=vm.createContext({document:{getElementById:id=>ids[id]},location:{reload(){state.reloads++;}},
    fetch:async(url,options)=>{state.sent.push({url,body:JSON.parse(options.body)});return typeof response==='function'?response(url):response;}});
  const code=html.match(/<script>([\s\S]*?)<\/script>/)[1].replace('{{ base|tojson }}',JSON.stringify('/empachecker/logger-review'));
  vm.runInContext(code,context);
  return {ids,state,submit:()=>ids['signin-form'].onsubmit({preventDefault(){}})};
}
(async()=>{
  assert(/id="signin" type="submit">Sign In<\/button>/.test(html));
  assert(/id="password" type="password"/.test(html));
  assert(!/code-form|Verify code/.test(html), 'the emailed code step is gone');
  let resolve;
  const app=boot(new Promise(r=>{resolve=r;}));
  app.ids.email.value='duyn@sccwrp.org'; app.ids.password.value='pw';
  const waiting=app.submit();
  assert.equal(app.ids.signin.disabled,true);
  resolve({ok:true,json:async()=>({email:'duyn@sccwrp.org'})});
  await waiting;
  assert.equal(app.state.reloads,1);
  assert.equal(app.state.sent[0].url,'/empachecker/logger-review/api/sign-in');
  assert.deepEqual(app.state.sent[0].body,{email:'duyn@sccwrp.org',password:'pw'});
  for(const [status,message] of [[401,'That password is not correct.'],[403,'This email is not authorized to use logger review.'],[429,'Too many wrong passwords. Try again in 15 minutes.']]) {
    const denied=boot({ok:false,status,json:async()=>({error:{message}})});
    denied.ids.email.value='duyn@sccwrp.org'; denied.ids.password.value='bad';
    await denied.submit();
    assert.equal(denied.state.reloads,0);
    assert.equal(denied.ids.message.textContent,message);
    assert.equal(denied.ids.message.hidden,false);
    assert.equal(denied.ids.password.value,'');
    assert.equal(denied.ids.signin.disabled,false);
  }
  console.log('sign-in page tests passed');
})().catch(e=>{console.error(e);process.exit(1);});
