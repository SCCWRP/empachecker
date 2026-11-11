const fs=require('fs'),path=require('path'),vm=require('vm'),assert=require('assert/strict');
const html=fs.readFileSync(path.resolve(__dirname,'../../proj/templates/logger_review_signin.html'),'utf8');
function boot(response) {
  const ids={};
  for(const [,id] of html.matchAll(/id="([^"]+)"/g)) ids[id]={hidden:false,value:'',disabled:false,textContent:'',focus(){},classList:{toggle(){}}};
  ids['code-form'].hidden=/<form id="code-form" hidden>/.test(html);
  const state={sent:[],reloads:0};
  const context=vm.createContext({document:{getElementById:id=>ids[id]},location:{reload(){state.reloads++;}},
    fetch:async(url,options)=>{state.sent.push({url,body:JSON.parse(options.body)});return response;}});
  const code=html.match(/<script>([\s\S]*?)<\/script>/)[1].replace('{{ base|tojson }}',JSON.stringify('/empachecker/logger-review'));
  vm.runInContext(code,context);
  return {ids,state,submit:()=>ids['email-form'].onsubmit({preventDefault(){}})};
}
(async()=>{
  assert(/\[hidden\]\s*\{\s*display:none\s*!important/.test(html), 'hidden code form must override the form display style');
  assert(/id="send" type="submit">Sign In<\/button>/.test(html));
  let resolve;
  const pending=new Promise(r=>{resolve=r;});
  const app=boot(pending);
  assert.equal(app.ids['code-form'].hidden,true);
  app.ids.email.value='duyn@sccwrp.org';
  const waiting=app.submit();
  assert.equal(app.ids.send.disabled,true);
  assert.equal(app.ids['code-form'].hidden,true);
  resolve({ok:true,json:async()=>({message:'A sign-in code was sent to your email.'})});
  await waiting;
  assert.equal(app.ids['code-form'].hidden,false);
  // A normal approved response advances only after the server has sent the code.
  const allowed=boot({ok:true,json:async()=>({message:'A sign-in code was sent to your email.'})});
  allowed.ids.email.value='duyn@sccwrp.org';
  await allowed.submit();
  assert.equal(allowed.ids['code-form'].hidden,false);
  assert.equal(allowed.ids.send.disabled,false);
  assert.equal(allowed.state.sent[0].body.email,'duyn@sccwrp.org');
  assert.equal(allowed.state.sent[0].url,'/empachecker/logger-review/api/request-code');
  for(const [status,message] of [[403,'This email is not authorized to use logger review.'],[500,'The code could not be sent. Try again in a minute.']]) {
    const denied=boot({ok:false,status,json:async()=>({error:{message}})});
    denied.ids['code-form'].hidden=false; // A previous successful attempt must not leave the code field open.
    denied.ids.email.value='someone@example.org';
    await denied.submit();
    assert.equal(denied.ids['code-form'].hidden,true);
    assert.equal(denied.ids.message.textContent,message);
    assert.equal(denied.ids.message.hidden,false);
    assert.equal(denied.ids.send.disabled,false);
  }
  console.log('sign-in page tests passed');
})().catch(e=>{console.error(e);process.exit(1);});
