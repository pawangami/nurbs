var iw=Object.defineProperty,rw=Object.defineProperties;var sw=Object.getOwnPropertyDescriptors;var Bl=Object.getOwnPropertySymbols;var v_=Object.prototype.hasOwnProperty,y_=Object.prototype.propertyIsEnumerable;var g_=(n,e,t)=>e in n?iw(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,Ae=(n,e)=>{for(var t in e||={})v_.call(e,t)&&g_(n,t,e[t]);if(Bl)for(var t of Bl(e))y_.call(e,t)&&g_(n,t,e[t]);return n},ke=(n,e)=>rw(n,sw(e));var Ep=(n,e)=>{var t={};for(var i in n)v_.call(n,i)&&e.indexOf(i)<0&&(t[i]=n[i]);if(n!=null&&Bl)for(var i of Bl(n))e.indexOf(i)<0&&y_.call(n,i)&&(t[i]=n[i]);return t};var cn=(n,e,t)=>new Promise((i,r)=>{var s=c=>{try{a(t.next(c))}catch(l){r(l)}},o=c=>{try{a(t.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(s,o);a((t=t.apply(n,e)).next())});var _n=null,Vl=!1,Sp=1,ow=null,Fn=Symbol("SIGNAL");function Ne(n){let e=_n;return _n=n,e}function jl(){return _n}var Cs={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Wl(n){if(Vl)throw new Error("");if(_n===null)return;_n.consumerOnSignalRead(n);let e=_n.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=_n.recomputing;if(i&&(t=e!==void 0?e.nextProducer:_n.producers,t!==void 0&&t.producer===n)){_n.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===_n&&(!i||cw(r,_n)))return;let s=go(_n),o={producer:n,consumer:_n,nextProducer:t,prevConsumer:void 0,lastReadVersion:n.version,nextConsumer:void 0};_n.producersTail=o,e!==void 0?e.nextProducer=o:_n.producers=o,s&&E_(n,o)}function __(){Sp++}function $l(n){if(!(go(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Sp)){if(!n.producerMustRecompute(n)&&!ql(n)){Gl(n);return}n.producerRecomputeValue(n),Gl(n)}}function Mp(n){if(n.consumers===void 0)return;let e=Vl;Vl=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||aw(i)}}finally{Vl=e}}function wp(){return _n?.consumerAllowSignalWrites!==!1}function aw(n){n.dirty=!0,Mp(n),n.consumerMarkedDirty?.(n)}function Gl(n){n.dirty=!1,n.lastCleanEpoch=Sp}function po(n){return n&&x_(n),Ne(n)}function x_(n){n.producersTail=void 0,n.recomputing=!0}function qa(n,e){Ne(e),n&&b_(n)}function b_(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(go(n))do t=Cp(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function ql(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||($l(t),i!==t.version))return!0}return!1}function mo(n){if(go(n)){let e=n.producers;for(;e!==void 0;)e=Cp(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function E_(n,e){let t=n.consumersTail,i=go(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)E_(r.producer,r)}function Cp(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!go(e)){let s=e.producers;for(;s!==void 0;)s=Cp(s)}return t}function go(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function Xl(n){ow?.(n)}function cw(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function Yl(n,e){return Object.is(n,e)}function Zl(n,e){let t=Object.create(lw);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if($l(t),Wl(t),t.value===$a)throw t.error;return t.value};return i[Fn]=t,Xl(t),i}var Hl=Symbol("UNSET"),zl=Symbol("COMPUTING"),$a=Symbol("ERRORED"),lw=ke(Ae({},Cs),{value:Hl,dirty:!0,error:null,equal:Yl,kind:"computed",producerMustRecompute(n){return n.value===Hl||n.value===zl},producerRecomputeValue(n){if(n.value===zl)throw new Error("");let e=n.value;n.value=zl;let t=po(n),i,r=!1;try{i=n.computation(),Ne(null),r=e!==Hl&&e!==$a&&i!==$a&&n.equal(e,i)}catch(s){i=$a,n.error=s}finally{qa(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function uw(){throw new Error}var S_=uw;function M_(n){S_(n)}function Tp(n){S_=n}var dw=null;function Dp(n,e){let t=Object.create(C_);t.value=n,e!==void 0&&(t.equal=e);let i=()=>w_(t);return i[Fn]=t,Xl(t),[i,o=>Kl(t,o),o=>Ap(t,o)]}function w_(n){return Wl(n),n.value}function Kl(n,e){wp()||M_(n),n.equal(n.value,e)||(n.value=e,fw(n))}function Ap(n,e){wp()||M_(n),Kl(n,e(n.value))}var C_=ke(Ae({},Cs),{equal:Yl,value:void 0,kind:"signal"});function fw(n){n.version++,__(),Mp(n),dw?.(n)}var Ip=ke(Ae({},Cs),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Rp(n){if(n.dirty=!1,n.version>0&&!ql(n))return;n.version++;let e=po(n);try{n.cleanup(),n.fn()}finally{qa(n,e)}}var Np;function Jl(){return Np}function Fi(n){let e=Np;return Np=n,e}var T_=Symbol("NotFound");function vo(n){return n===T_||n?.name==="\u0275NotFound"}function D_(n){let e=Ne(null);try{return n()}finally{Ne(e)}}function ht(n){return typeof n=="function"}function Ql(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var eu=Ql(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Xa(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var Ft=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(ht(i))try{i()}catch(s){e=s instanceof eu?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{A_(s)}catch(o){e=e??[],o instanceof eu?e=[...e,...o.errors]:e.push(o)}}if(e)throw new eu(e)}}add(e){var t;if(e&&e!==this)if(this.closed)A_(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Xa(t,e)}remove(e){let{_finalizers:t}=this;t&&Xa(t,e),e instanceof n&&e._removeParent(this)}};Ft.EMPTY=(()=>{let n=new Ft;return n.closed=!0,n})();var Pp=Ft.EMPTY;function tu(n){return n instanceof Ft||n&&"closed"in n&&ht(n.remove)&&ht(n.add)&&ht(n.unsubscribe)}function A_(n){ht(n)?n():n.unsubscribe()}var pi={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var yo={setTimeout(n,e,...t){let{delegate:i}=yo;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=yo;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function nu(n){yo.setTimeout(()=>{let{onUnhandledError:e}=pi;if(e)e(n);else throw n})}function Op(){}var I_=Fp("C",void 0,void 0);function R_(n){return Fp("E",void 0,n)}function N_(n){return Fp("N",n,void 0)}function Fp(n,e,t){return{kind:n,value:e,error:t}}var Ts=null;function _o(n){if(pi.useDeprecatedSynchronousErrorHandling){let e=!Ts;if(e&&(Ts={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Ts;if(Ts=null,t)throw i}}else n()}function P_(n){pi.useDeprecatedSynchronousErrorHandling&&Ts&&(Ts.errorThrown=!0,Ts.error=n)}var Ds=class extends Ft{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,tu(e)&&e.add(this)):this.destination=mw}static create(e,t,i){return new xo(e,t,i)}next(e){this.isStopped?kp(N_(e),this):this._next(e)}error(e){this.isStopped?kp(R_(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?kp(I_,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},hw=Function.prototype.bind;function Lp(n,e){return hw.call(n,e)}var Up=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){iu(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){iu(i)}else iu(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){iu(t)}}},xo=class extends Ds{constructor(e,t,i){super();let r;if(ht(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&pi.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Lp(e.next,s),error:e.error&&Lp(e.error,s),complete:e.complete&&Lp(e.complete,s)}):r=e}this.destination=new Up(r)}};function iu(n){pi.useDeprecatedSynchronousErrorHandling?P_(n):nu(n)}function pw(n){throw n}function kp(n,e){let{onStoppedNotification:t}=pi;t&&yo.setTimeout(()=>t(n,e))}var mw={closed:!0,next:Op,error:pw,complete:Op};var bo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function O_(n){return n}function F_(n){return n.length===0?O_:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Yt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=vw(t)?t:new xo(t,i,r);return _o(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=L_(i),new i((r,s)=>{let o=new xo({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[bo](){return this}pipe(...t){return F_(t)(this)}toPromise(t){return t=L_(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function L_(n){var e;return(e=n??pi.Promise)!==null&&e!==void 0?e:Promise}function gw(n){return n&&ht(n.next)&&ht(n.error)&&ht(n.complete)}function vw(n){return n&&n instanceof Ds||gw(n)&&tu(n)}function yw(n){return ht(n?.lift)}function Eo(n){return e=>{if(yw(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function So(n,e,t,i,r){return new Bp(n,e,t,i,r)}var Bp=class extends Ds{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var k_=Ql(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var mi=(()=>{class n extends Yt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new ru(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new k_}next(t){_o(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){_o(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){_o(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?Pp:(this.currentObservers=null,s.push(t),new Ft(()=>{this.currentObservers=null,Xa(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new Yt;return t.source=this,t}}return n.create=(e,t)=>new ru(e,t),n})(),ru=class extends mi{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Pp}};var lr=class extends mi{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function _w(n){return n[n.length-1]}function U_(n){return ht(_w(n))?n.pop():void 0}function V_(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{l(i.next(u))}catch(d){o(d)}}function c(u){try{l(i.throw(u))}catch(d){o(d)}}function l(u){u.done?s(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function B_(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function As(n){return this instanceof As?(this.v=n,this):new As(n)}function H_(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(h){return function(g){return Promise.resolve(g).then(h,d)}}function a(h,g){i[h]&&(r[h]=function(y){return new Promise(function(p,m){s.push([h,y,p,m])>1||c(h,y)})},g&&(r[h]=g(r[h])))}function c(h,g){try{l(i[h](g))}catch(y){f(s[0][3],y)}}function l(h){h.value instanceof As?Promise.resolve(h.value.v).then(u,d):f(s[0][2],h)}function u(h){c("next",h)}function d(h){c("throw",h)}function f(h,g){h(g),s.shift(),s.length&&c(s[0][0],s[0][1])}}function z_(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof B_=="function"?B_(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,c){o=n[s](o),r(a,c,o.done,o.value)})}}function r(s,o,a,c){Promise.resolve(c).then(function(l){s({value:l,done:a})},o)}}var su=n=>n&&typeof n.length=="number"&&typeof n!="function";function ou(n){return ht(n?.then)}function au(n){return ht(n[bo])}function cu(n){return Symbol.asyncIterator&&ht(n?.[Symbol.asyncIterator])}function lu(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function xw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var uu=xw();function du(n){return ht(n?.[uu])}function fu(n){return H_(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield As(t.read());if(r)return yield As(void 0);yield yield As(i)}}finally{t.releaseLock()}})}function hu(n){return ht(n?.getReader)}function kr(n){if(n instanceof Yt)return n;if(n!=null){if(au(n))return bw(n);if(su(n))return Ew(n);if(ou(n))return Sw(n);if(cu(n))return G_(n);if(du(n))return Mw(n);if(hu(n))return ww(n)}throw lu(n)}function bw(n){return new Yt(e=>{let t=n[bo]();if(ht(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Ew(n){return new Yt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function Sw(n){return new Yt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,nu)})}function Mw(n){return new Yt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function G_(n){return new Yt(e=>{Cw(n,e).catch(t=>e.error(t))})}function ww(n){return G_(fu(n))}function Cw(n,e){var t,i,r,s;return V_(this,void 0,void 0,function*(){try{for(t=z_(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function Li(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function pu(n,e=0){return Eo((t,i)=>{t.subscribe(So(i,r=>Li(i,n,()=>i.next(r),e),()=>Li(i,n,()=>i.complete(),e),r=>Li(i,n,()=>i.error(r),e)))})}function mu(n,e=0){return Eo((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function j_(n,e){return kr(n).pipe(mu(e),pu(e))}function W_(n,e){return kr(n).pipe(mu(e),pu(e))}function $_(n,e){return new Yt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function q_(n,e){return new Yt(t=>{let i;return Li(t,e,()=>{i=n[uu](),Li(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>ht(i?.return)&&i.return()})}function gu(n,e){if(!n)throw new Error("Iterable cannot be null");return new Yt(t=>{Li(t,e,()=>{let i=n[Symbol.asyncIterator]();Li(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function X_(n,e){return gu(fu(n),e)}function Y_(n,e){if(n!=null){if(au(n))return j_(n,e);if(su(n))return $_(n,e);if(ou(n))return W_(n,e);if(cu(n))return gu(n,e);if(du(n))return q_(n,e);if(hu(n))return X_(n,e)}throw lu(n)}function Vp(n,e){return e?Y_(n,e):kr(n)}function Is(n,e){return Eo((t,i)=>{let r=0;t.subscribe(So(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:Tw}=Array;function Dw(n,e){return Tw(e)?n(...e):n(e)}function Z_(n){return Is(e=>Dw(n,e))}var{isArray:Aw}=Array,{getPrototypeOf:Iw,prototype:Rw,keys:Nw}=Object;function K_(n){if(n.length===1){let e=n[0];if(Aw(e))return{args:e,keys:null};if(Pw(e)){let t=Nw(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Pw(n){return n&&typeof n=="object"&&Iw(n)===Rw}function J_(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Hp(...n){let e=U_(n),{args:t,keys:i}=K_(n),r=new Yt(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),c=o,l=o;for(let u=0;u<o;u++){let d=!1;kr(t[u]).subscribe(So(s,f=>{d||(d=!0,l--),a[u]=f},()=>c--,void 0,()=>{(!c||!d)&&(l||s.next(i?J_(i,a):a),s.complete())}))}});return e?r.pipe(Z_(e)):r}var em="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",ze=class extends Error{code;constructor(e,t){super(Su(e,t)),this.code=e}};function Ow(n){return`NG0${Math.abs(n)}`}function Su(n,e){return`${Ow(n)}${e?": "+e:""}`}function bt(n){for(let e in n)if(n[e]===bt)return e;throw Error("")}function i0(n,e){for(let t in e)e.hasOwnProperty(t)&&!n.hasOwnProperty(t)&&(n[t]=e[t])}function Mu(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Mu).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function tm(n,e){return n?e?`${n} ${e}`:n:e||""}var Fw=bt({__forward_ref__:bt});function Hr(n){return n.__forward_ref__=Hr,n}function ln(n){return nm(n)?n():n}function nm(n){return typeof n=="function"&&n.hasOwnProperty(Fw)&&n.__forward_ref__===Hr}function Ut(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Fs(n){return{providers:n.providers||[],imports:n.imports||[]}}function wu(n){return Lw(n,Cu)}function Lw(n,e){return n.hasOwnProperty(e)&&n[e]||null}function kw(n){let e=n?.[Cu]??null;return e||null}function Gp(n){return n&&n.hasOwnProperty(yu)?n[yu]:null}var Cu=bt({\u0275prov:bt}),yu=bt({\u0275inj:bt}),Ye=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Ut({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function im(n){return n&&!!n.\u0275providers}var rm=bt({\u0275cmp:bt}),sm=bt({\u0275dir:bt}),om=bt({\u0275pipe:bt});var Za=bt({\u0275fac:bt}),Ls=bt({__NG_ELEMENT_ID__:bt}),Q_=bt({__NG_ENV_ID__:bt});function ks(n){return cm(n,"@Component"),n[rm]||null}function am(n){return cm(n,"@Directive"),n[sm]||null}function r0(n){return cm(n,"@Pipe"),n[om]||null}function cm(n,e){if(n==null)throw new ze(-919,!1)}function hr(n){return typeof n=="string"?n:n==null?"":String(n)}var s0=bt({ngErrorCode:bt}),Uw=bt({ngErrorMessage:bt}),Bw=bt({ngTokenPath:bt});function lm(n,e){return o0("",-200,e)}function Tu(n,e){throw new ze(-201,!1)}function o0(n,e,t){let i=new ze(e,n);return i[s0]=e,i[Uw]=n,t&&(i[Bw]=t),i}function Vw(n){return n[s0]}var jp;function a0(){return jp}function In(n){let e=jp;return jp=n,e}function um(n,e,t){let i=wu(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Tu(n,"")}var Us=globalThis;var Hw={},Rs=Hw,zw="__NG_DI_FLAG__",Wp=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=Ns(t)||0;try{return this.injector.get(e,i&8?null:Rs,i)}catch(r){if(vo(r))return r;throw r}}};function Gw(n,e=0){let t=Jl();if(t===void 0)throw new ze(-203,!1);if(t===null)return um(n,void 0,e);{let i=jw(e),r=t.retrieve(n,i);if(vo(r)){if(i.optional)return null;throw r}return r}}function mt(n,e=0){return(a0()||Gw)(ln(n),e)}function et(n,e){return mt(n,Ns(e))}function Ns(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function jw(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function $p(n){let e=[];for(let t=0;t<n.length;t++){let i=ln(n[t]);if(Array.isArray(i)){if(i.length===0)throw new ze(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=Ww(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(mt(r,s))}else e.push(mt(i))}return e}function Ww(n){return n[zw]}function Ur(n,e){let t=n.hasOwnProperty(Za);return t?n[Za]:null}function c0(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function l0(n){return n.flat(Number.POSITIVE_INFINITY)}function Du(n,e){n.forEach(t=>Array.isArray(t)?Du(t,e):e(t))}function dm(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function ec(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function u0(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function d0(n,e,t){let i=Co(n,e);return i>=0?n[i|1]=t:(i=~i,u0(n,i,e,t)),i}function Au(n,e){let t=Co(n,e);if(t>=0)return n[t|1]}function Co(n,e){return $w(n,e,1)}function $w(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var zr={},$n=[],Bs=new Ye(""),tc=new Ye("",-1),fm=new Ye(""),wo=class{get(e,t=Rs){if(t===Rs){let r=o0("",-201);throw r.name="\u0275NotFound",r}return t}};function hm(n){return{\u0275providers:n}}function f0(...n){return{\u0275providers:pm(!0,n),\u0275fromNgModule:!0}}function pm(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return Du(e,o=>{let a=o;_u(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&h0(r,s),t}function h0(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];mm(r,s=>{e(s,i)})}}function _u(n,e,t,i){if(n=ln(n),!n)return!1;let r=null,s=Gp(n),o=!s&&ks(n);if(!s&&!o){let c=n.ngModule;if(s=Gp(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)_u(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;Du(s.imports,u=>{_u(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&h0(l,e)}if(!a){let l=Ur(r)||(()=>new r);e({provide:r,useFactory:l,deps:$n},r),e({provide:fm,useValue:r,multi:!0},r),e({provide:Bs,useValue:()=>mt(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;mm(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function mm(n,e){for(let t of n)im(t)&&(t=t.\u0275providers),Array.isArray(t)?mm(t,e):e(t)}var qw=bt({provide:String,useValue:bt});function p0(n){return n!==null&&typeof n=="object"&&qw in n}function Xw(n){return!!(n&&n.useExisting)}function Yw(n){return!!(n&&n.useFactory)}function Ps(n){return typeof n=="function"}function m0(n){return!!n.useClass}var nc=new Ye(""),vu={},e0={},zp;function ic(){return zp===void 0&&(zp=new wo),zp}var ii=class{},Os=class extends ii{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Xp(e,o=>this.processProvider(o)),this.records.set(tc,Mo(void 0,this)),r.has("environment")&&this.records.set(ii,Mo(void 0,this));let s=this.records.get(nc);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(fm,$n,{self:!0}))}retrieve(e,t){let i=Ns(t)||0;try{return this.get(e,Rs,i)}catch(r){if(vo(r))return r;throw r}}destroy(){Ya(this),this._destroyed=!0;let e=Ne(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ne(e)}}onDestroy(e){return Ya(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Ya(this);let t=Fi(this),i=In(void 0),r;try{return e()}finally{Fi(t),In(i)}}get(e,t=Rs,i){if(Ya(this),e.hasOwnProperty(Q_))return e[Q_](this);let r=Ns(i),s,o=Fi(this),a=In(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=eC(e)&&wu(e);u&&this.injectableDefInScope(u)?l=Mo(qp(e),vu):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?ic():this.parent;return t=r&8&&t===Rs?null:t,c.get(e,t)}catch(c){let l=Vw(c);throw l===-200||l===-201?new ze(l,null):c}finally{In(a),Fi(o)}}resolveInjectorInitializers(){let e=Ne(null),t=Fi(this),i=In(void 0),r;try{let s=this.get(Bs,$n,{self:!0});for(let o of s)o()}finally{Fi(t),In(i),Ne(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=ln(e);let t=Ps(e)?e:ln(e&&e.provide),i=Kw(e);if(!Ps(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Mo(void 0,vu,!0),r.factory=()=>$p(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=Ne(null);try{if(t.value===e0)throw lm("");return t.value===vu&&(t.value=e0,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&Qw(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ne(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=ln(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function qp(n){let e=wu(n),t=e!==null?e.factory:Ur(n);if(t!==null)return t;if(n instanceof Ye)throw new ze(-204,!1);if(n instanceof Function)return Zw(n);throw new ze(-204,!1)}function Zw(n){if(n.length>0)throw new ze(-204,!1);let t=kw(n);return t!==null?()=>t.factory(n):()=>new n}function Kw(n){if(p0(n))return Mo(void 0,n.useValue);{let e=gm(n);return Mo(e,vu)}}function gm(n,e,t){let i;if(Ps(n)){let r=ln(n);return Ur(r)||qp(r)}else if(p0(n))i=()=>ln(n.useValue);else if(Yw(n))i=()=>n.useFactory(...$p(n.deps||[]));else if(Xw(n))i=(r,s)=>mt(ln(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=ln(n&&(n.useClass||n.provide));if(Jw(n))i=()=>new r(...$p(n.deps));else return Ur(r)||qp(r)}return i}function Ya(n){if(n.destroyed)throw new ze(-205,!1)}function Mo(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function Jw(n){return!!n.deps}function Qw(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function eC(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Xp(n,e){for(let t of n)Array.isArray(t)?Xp(t,e):t&&im(t)?Xp(t.\u0275providers,e):e(t)}function Iu(n,e){let t;n instanceof Os?(Ya(n),t=n):t=new Wp(n);let i,r=Fi(t),s=In(void 0);try{return e()}finally{Fi(r),In(s)}}function g0(){return a0()!==void 0||Jl()!=null}var vi=0,Ie=1,Ue=2,Qt=3,ri=4,qn=5,To=6,Do=7,zt=8,ki=9,Ui=10,Nt=11,Ao=12,vm=13,Vs=14,Ln=15,Gr=16,Hs=17,Bi=18,Vi=19,ym=20,ur=21,Ru=22,Br=23,Xn=24,Nu=25,Hi=26,en=27,v0=1,_m=6,jr=7,rc=8,zs=9,Bt=10;function pr(n){return Array.isArray(n)&&typeof n[v0]=="object"}function si(n){return Array.isArray(n)&&n[v0]===!0}function xm(n){return(n.flags&4)!==0}function mr(n){return n.componentOffset>-1}function Pu(n){return(n.flags&1)===1}function zi(n){return!!n.template}function Io(n){return(n[Ue]&512)!==0}function Gs(n){return(n[Ue]&256)===256}var bm="svg",y0="math";function oi(n){for(;Array.isArray(n);)n=n[vi];return n}function Em(n,e){return oi(e[n])}function ai(n,e){return oi(e[n.index])}function Ou(n,e){return n.data[e]}function _0(n,e){return n[e]}function Sm(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function ci(n,e){let t=e[n];return pr(t)?t:t[vi]}function x0(n){return(n[Ue]&4)===4}function Fu(n){return(n[Ue]&128)===128}function b0(n){return si(n[Qt])}function Gi(n,e){return e==null?null:n[e]}function Mm(n){n[Hs]=0}function wm(n){n[Ue]&1024||(n[Ue]|=1024,Fu(n)&&Ro(n))}function E0(n,e){for(;n>0;)e=e[Vs],n--;return e}function sc(n){return!!(n[Ue]&9216||n[Xn]?.dirty)}function Lu(n){n[Ui].changeDetectionScheduler?.notify(8),n[Ue]&64&&(n[Ue]|=1024),sc(n)&&Ro(n)}function Ro(n){n[Ui].changeDetectionScheduler?.notify(0);let e=dr(n);for(;e!==null&&!(e[Ue]&8192||(e[Ue]|=8192,!Fu(e)));)e=dr(e)}function ku(n,e){if(Gs(n))throw new ze(911,!1);n[ur]===null&&(n[ur]=[]),n[ur].push(e)}function S0(n,e){if(n[ur]===null)return;let t=n[ur].indexOf(e);t!==-1&&n[ur].splice(t,1)}function dr(n){let e=n[Qt];return si(e)?e[Qt]:e}function Cm(n){return n[Do]??=[]}function Tm(n){return n.cleanup??=[]}function M0(n,e,t,i){let r=Cm(e);r.push(t),n.firstCreatePass&&Tm(n).push(i,r.length-1)}var Ze={lFrame:U0(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Yp=!1;function w0(){return Ze.lFrame.elementDepthCount}function C0(){Ze.lFrame.elementDepthCount++}function Dm(){Ze.lFrame.elementDepthCount--}function T0(){return Ze.bindingsEnabled}function D0(){return Ze.skipHydrationRootTNode!==null}function Am(n){return Ze.skipHydrationRootTNode===n}function Im(){Ze.skipHydrationRootTNode=null}function Ge(){return Ze.lFrame.lView}function Gt(){return Ze.lFrame.tView}function Ct(n){return Ze.lFrame.contextLView=n,n[zt]}function Tt(n){return Ze.lFrame.contextLView=null,n}function un(){let n=Rm();for(;n!==null&&n.type===64;)n=n.parent;return n}function Rm(){return Ze.lFrame.currentTNode}function A0(){let n=Ze.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function No(n,e){let t=Ze.lFrame;t.currentTNode=n,t.isParent=e}function Nm(){return Ze.lFrame.isParent}function I0(){Ze.lFrame.isParent=!1}function Pm(){return Yp}function Ka(n){let e=Yp;return Yp=n,e}function R0(){let n=Ze.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function Om(){return Ze.lFrame.bindingIndex}function N0(n){return Ze.lFrame.bindingIndex=n}function Wr(){return Ze.lFrame.bindingIndex++}function Uu(n){let e=Ze.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function P0(){return Ze.lFrame.inI18n}function O0(n,e){let t=Ze.lFrame;t.bindingIndex=t.bindingRootIndex=n,Bu(e)}function F0(){return Ze.lFrame.currentDirectiveIndex}function Bu(n){Ze.lFrame.currentDirectiveIndex=n}function L0(n){let e=Ze.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Fm(){return Ze.lFrame.currentQueryIndex}function Vu(n){Ze.lFrame.currentQueryIndex=n}function tC(n){let e=n[Ie];return e.type===2?e.declTNode:e.type===1?n[qn]:null}function Lm(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=tC(s),r===null||(s=s[Vs],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Ze.lFrame=k0();return i.currentTNode=e,i.lView=n,!0}function Hu(n){let e=k0(),t=n[Ie];Ze.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function k0(){let n=Ze.lFrame,e=n===null?null:n.child;return e===null?U0(n):e}function U0(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function B0(){let n=Ze.lFrame;return Ze.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var km=B0;function zu(){let n=B0();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function V0(n){return(Ze.lFrame.contextLView=E0(n,Ze.lFrame.contextLView))[zt]}function ji(){return Ze.lFrame.selectedIndex}function $r(n){Ze.lFrame.selectedIndex=n}function Po(){let n=Ze.lFrame;return Ou(n.tView,n.selectedIndex)}function Oo(){Ze.lFrame.currentNamespace=bm}function Gu(){nC()}function nC(){Ze.lFrame.currentNamespace=null}function Um(){return Ze.lFrame.currentNamespace}var H0=!0;function ju(){return H0}function Wu(n){H0=n}function Zp(n,e=null,t=null,i){let r=z0(n,e,t,i);return r.resolveInjectorInitializers(),r}function z0(n,e=null,t=null,i,r=new Set){let s=[t||$n,f0(n)],o;return new Os(s,e||ic(),o||null,r)}var gi=class n{static THROW_IF_NOT_FOUND=Rs;static NULL=new wo;static create(e,t){if(Array.isArray(e))return Zp({name:""},t,e,"");{let i=e.name??"";return Zp({name:i},e.parent,e.providers,i)}}static \u0275prov=Ut({token:n,providedIn:"any",factory:()=>mt(tc)});static __NG_ELEMENT_ID__=-1},li=new Ye(""),js=(()=>{class n{static __NG_ELEMENT_ID__=iC;static __NG_ENV_ID__=t=>t}return n})(),xu=class extends js{_lView;constructor(e){super(),this._lView=e}get destroyed(){return Gs(this._lView)}onDestroy(e){let t=this._lView;return ku(t,e),()=>S0(t,e)}};function iC(){return new xu(Ge())}var Bm=!1,G0=new Ye(""),Ws=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new lr(!1);debugTaskTracker=et(G0,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new Yt(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Ut({token:n,providedIn:"root",factory:()=>new n})}return n})(),Kp=class extends mi{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,g0()&&(this.destroyRef=et(js,{optional:!0})??void 0,this.pendingTasks=et(Ws,{optional:!0})??void 0)}emit(e){let t=Ne(null);try{super.next(e)}finally{Ne(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof Ft&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},xt=Kp;function bu(...n){}function Vm(n){let e,t;function i(){n=bu;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch(r){}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function j0(n){return queueMicrotask(()=>n()),()=>{n=bu}}var Hm="isAngularZone",Ja=Hm+"_ID",rC=0,Rt=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new xt(!1);onMicrotaskEmpty=new xt(!1);onStable=new xt(!1);onError=new xt(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=Bm}=e;if(typeof Zone>"u")throw new ze(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,aC(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Hm)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new ze(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new ze(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,sC,bu,bu);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},sC={};function zm(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function oC(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Vm(()=>{n.callbackScheduled=!1,Jp(n),n.isCheckStableRunning=!0,zm(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Jp(n)}function aC(n){let e=()=>{oC(n)},t=rC++;n._inner=n._inner.fork({name:"angular",properties:{[Hm]:!0,[Ja]:t,[Ja+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(cC(c))return i.invokeTask(s,o,a,c);try{return t0(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),n0(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return t0(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!lC(c)&&e(),n0(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Jp(n),zm(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Jp(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function t0(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function n0(n){n._nesting--,zm(n)}var Qa=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new xt;onMicrotaskEmpty=new xt;onStable=new xt;onError=new xt;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function cC(n){return W0(n,"__ignore_ng_zone__")}function lC(n){return W0(n,"__scheduler_tick__")}function W0(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var fr=class{_console=console;handleError(e){this._console.error("ERROR",e)}},qr=new Ye("",{factory:()=>{let n=et(Rt),e=et(ii),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(fr),t.handleError(i))})}}}),$0={provide:Bs,useValue:()=>{let n=et(fr,{optional:!0})},multi:!0};function $s(n,e){let[t,i,r]=Dp(n,e?.equal),s=t,o=s[Fn];return s.set=i,s.update=r,s.asReadonly=q0.bind(s),s}function q0(){let n=this[Fn];if(n.readonlyFn===void 0){let e=()=>this();e[Fn]=n,n.readonlyFn=e}return n.readonlyFn}var oc=new Ye("",{factory:()=>uC}),uC="ng";var $u=new Ye(""),ac=new Ye("",{providedIn:"platform",factory:()=>"unknown"});var qu=new Ye("",{factory:()=>et(li).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Xu=(()=>{class n{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=dC}return n})();function dC(){return new Xu(Ge(),un())}var Vr=class{},Fo=new Ye("",{factory:()=>!0});var Yu=new Ye(""),Zu=(()=>{class n{static \u0275prov=Ut({token:n,providedIn:"root",factory:()=>new Qp})}return n})(),Qp=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Eu=class{[Fn];constructor(e){this[Fn]=e}destroy(){this[Fn].destroy()}};function Gm(n,e){let t=e?.injector??et(gi),i=e?.manualCleanup!==!0?t.get(js):null,r,s=t.get(Xu,null,{optional:!0}),o=t.get(Vr);return s!==null?(r=pC(s.view,o,n),i instanceof xu&&i._lView===s.view&&(i=null)):r=mC(n,t.get(Zu),o),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Eu(r)}var X0=ke(Ae({},Ip),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=Ka(!1);try{Rp(this)}finally{Ka(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=Ne(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],Ne(n)}}}),fC=ke(Ae({},X0),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(mo(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),hC=ke(Ae({},X0),{consumerMarkedDirty(){this.view[Ue]|=8192,Ro(this.view),this.notifier.notify(13)},destroy(){if(mo(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[Br]?.delete(this)}});function pC(n,e,t){let i=Object.create(hC);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=e,i.fn=Y0(i,t),n[Br]??=new Set,n[Br].add(i),i.consumerMarkedDirty(i),i}function mC(n,e,t){let i=Object.create(fC);return i.fn=Y0(i,n),i.scheduler=e,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function Y0(n,e){return()=>{e(t=>(n.cleanupFns??=[]).push(t))}}function Ku(n){return typeof n=="function"&&n[Fn]!==void 0}function Ju(n){return Ku(n)&&typeof n.set=="function"}function xc(n){return{toString:n}.toString()}var rd=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function Tx(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var gr=(()=>{let n=()=>Dx;return n.ngInherit=!0,n})();function Dx(n){return n.type.prototype.ngOnChanges&&(n.setInput=IC),AC}function AC(){let n=Ax(this),e=n?.current;if(e){let t=n.previous;if(t===zr)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function IC(n,e,t,i,r){let s=this.declaredInputs[i],o=Ax(n)||RC(n,{previous:zr,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new rd(l&&l.currentValue,t,c===zr),Tx(n,e,r,t)}var tg="__ngSimpleChanges__";function Ax(n){return Object.hasOwn(n,tg)&&n[tg]||null}function RC(n,e){return n[tg]=e}var Z0=[];var Et=function(n,e=null,t){for(let i=0;i<Z0.length;i++){let r=Z0[i];r(n,e,t)}},ft=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(ft||{});function NC(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=Dx(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function PC(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function ed(n,e,t){Ix(n,e,3,t)}function td(n,e,t,i){(n[Ue]&3)===t&&Ix(n,e,t,i)}function jm(n,e){let t=n[Ue];(t&3)===e&&(t&=16383,t+=1,n[Ue]=t)}function Ix(n,e,t,i){let r=i!==void 0?n[Hs]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Hs]+=65536),(a<s||s==-1)&&(OC(n,t,e,c),n[Hs]=(n[Hs]&4294901760)+c+2),c++}function K0(n,e){Et(ft.LifecycleHookStart,n,e);let t=Ne(null);try{e.call(n)}finally{Ne(t),Et(ft.LifecycleHookEnd,n,e)}}function OC(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Ue]>>14<n[Hs]>>16&&(n[Ue]&3)===e&&(n[Ue]+=16384,K0(a,s)):K0(a,s)}var ko=-1,Xs=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function FC(n){return(n.flags&8)!==0}function LC(n){return(n.flags&16)!==0}function kC(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];BC(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function UC(n){return n===3||n===4||n===6}function BC(n){return n.charCodeAt(0)===64}function Uo(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?J0(n,t,r,null,e[++i]):J0(n,t,r,null,null))}}return n}function J0(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function Rx(n){return n!==ko}function sd(n){return n&32767}function VC(n){return n>>16}function od(n,e){let t=VC(n),i=e;for(;t>0;)i=i[Vs],t--;return i}var ng=!0;function ad(n){let e=ng;return ng=n,e}var HC=256,Nx=HC-1,Px=5,zC=0,Wi={};function GC(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Ls)&&(i=t[Ls]),i==null&&(i=t[Ls]=zC++);let r=i&Nx,s=1<<r;e.data[n+(r>>Px)]|=s}function cd(n,e){let t=Ox(n,e);if(t!==-1)return t;let i=e[Ie];i.firstCreatePass&&(n.injectorIndex=e.length,Wm(i.data,n),Wm(e,null),Wm(i.blueprint,null));let r=Ig(n,e),s=n.injectorIndex;if(Rx(r)){let o=sd(r),a=od(r,e),c=a[Ie].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Wm(n,e){n.push(0,0,0,0,0,0,0,0,e)}function Ox(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function Ig(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Bx(r),i===null)return ko;if(t++,r=r[Vs],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return ko}function ig(n,e,t){GC(n,e,t)}function Fx(n,e,t){if(t&8||n!==void 0)return n;Tu(e,"NodeInjector")}function Lx(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[ki],s=In(void 0);try{return r?r.get(e,i,t&8):um(e,i,t&8)}finally{In(s)}}return Fx(i,e,t)}function kx(n,e,t,i=0,r){if(n!==null){if(e[Ue]&2048&&!(i&2)){let o=qC(n,e,t,i,Wi);if(o!==Wi)return o}let s=Ux(n,e,t,i,Wi);if(s!==Wi)return s}return Lx(e,t,i,r)}function Ux(n,e,t,i,r){let s=WC(t);if(typeof s=="function"){if(!Lm(e,n,i))return i&1?Fx(r,t,i):Lx(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))Tu(t);else return o}finally{km()}}else if(typeof s=="number"){let o=null,a=Ox(n,e),c=ko,l=i&1?e[Ln][qn]:null;for((a===-1||i&4)&&(c=a===-1?Ig(n,e):e[a+8],c===ko||!ex(i,!1)?a=-1:(o=e[Ie],a=sd(c),e=od(c,e)));a!==-1;){let u=e[Ie];if(Q0(s,a,u.data)){let d=jC(a,e,t,o,i,l);if(d!==Wi)return d}c=e[a+8],c!==ko&&ex(i,e[Ie].data[a+8]===l)&&Q0(s,a,e)?(o=u,a=sd(c),e=od(c,e)):a=-1}}return r}function jC(n,e,t,i,r,s){let o=e[Ie],a=o.data[n+8],c=i==null?mr(a)&&ng:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=nd(a,o,t,c,l);return u!==null?fc(e,o,u,a,r):Wi}function nd(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,d=i?a:a+u,f=r?a+u:l;for(let h=d;h<f;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&zi(h)&&h.type===t)return c}return null}function fc(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof Xs){let a=s;if(a.resolving)throw lm("");let c=ad(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,d=a.injectImpl?In(a.injectImpl):null,f=Lm(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&NC(t,o[t],e)}finally{d!==null&&In(d),ad(c),a.resolving=!1,km()}}return s}function WC(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Ls)?n[Ls]:void 0;return typeof e=="number"?e>=0?e&Nx:$C:e}function Q0(n,e,t){let i=1<<n;return!!(t[e+(n>>Px)]&i)}function ex(n,e){return!(n&2)&&!(n&1&&e)}var Xr=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return kx(this._tNode,this._lView,e,Ns(i),t)}};function $C(){return new Xr(un(),Ge())}function bd(n){return xc(()=>{let e=n.prototype.constructor,t=e[Za]||rg(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[Za]||rg(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function rg(n){return nm(n)?()=>{let e=rg(ln(n));return e&&e()}:Ur(n)}function qC(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Ue]&2048&&!Io(o);){let a=Ux(s,o,t,i|2,Wi);if(a!==Wi)return a;let c=s.parent;if(!c){let l=o[ym];if(l){let u=l.get(t,Wi,i&-5);if(u!==Wi)return u}c=Bx(o),o=o[Vs]}s=c}return r}function Bx(n){let e=n[Ie],t=e.type;return t===2?e.declTNode:t===1?n[qn]:null}function Kr(n){return{token:n.token,providedIn:n.autoProvided===!1?null:"root",factory:n.factory,value:void 0}}function XC(){return zo(un(),Ge())}function zo(n,e){return new vr(ai(n,e))}var vr=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=XC}return n})();function YC(n){return n instanceof vr?n.nativeElement:n}function ZC(){return this._results[Symbol.iterator]()}var ld=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new mi}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=l0(e);(this._changesDetected=!c0(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=ZC};function Vx(n){return(n.flags&128)===128}var Rg=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(Rg||{}),Hx=new Map,KC=0;function JC(){return KC++}function QC(n){Hx.set(n[Vi],n)}function sg(n){Hx.delete(n[Vi])}var tx="__ngContext__";function Bo(n,e){pr(e)?(n[tx]=e[Vi],QC(e)):n[tx]=e}function zx(n){return jx(n[Ao])}function Gx(n){return jx(n[ri])}function jx(n){for(;n!==null&&!si(n);)n=n[ri];return n}var og;function Ng(n){og=n}function Wx(){if(og!==void 0)return og;if(typeof document<"u")return document;throw new ze(210,!1)}var $x="r";var qx="di";var Xx=!1,Yx=new Ye("",{factory:()=>Xx});var nx=new WeakMap;function eT(n,e){if(n==null||typeof n!="object")return;let t=nx.get(n);t||(t=new WeakSet,nx.set(n,t)),t.add(e)}var tT=(n,e,t,i)=>{};function nT(n,e,t,i){tT(n,e,t,i)}function Pg(n){return(n.flags&32)===32}var iT=()=>null;function Zx(n,e,t=!1){return iT(n,e,t)}function Kx(n,e){let t=n.contentQueries;if(t!==null){let i=Ne(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Vu(s),a.contentQueries(2,e[o],o)}}}finally{Ne(i)}}}function ag(n,e,t){Vu(0);let i=Ne(null);try{e(n,t)}finally{Ne(i)}}function Jx(n,e,t){if(xm(e)){let i=Ne(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{Ne(i)}}}var _i=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(_i||{});var cg=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${em})`}};function Qx(n){return n instanceof cg?n.changingThisBreaksApplicationSecurity:n}function rT(n,e){return n.createText(e)}function sT(n,e,t){n.setValue(e,t)}function eb(n,e,t){return n.createElement(e,t)}function ud(n,e,t,i,r){n.insertBefore(e,t,i,r)}function tb(n,e,t){n.appendChild(e,t)}function ix(n,e,t,i,r){i!==null?ud(n,e,t,i,r):tb(n,e,t)}function nb(n,e,t,i){n.removeChild(null,e,t,i)}function oT(n,e,t){n.setAttribute(e,"style",t)}function aT(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function ib(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&kC(n,e,i),r!==null&&aT(n,e,r),s!==null&&oT(n,e,s)}function cT(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var rb="ng-template";function lT(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&cT(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Og(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function Og(n){return n.type===4&&n.value!==rb}function uT(n,e,t){let i=n.type===4&&!t?rb:n.value;return e===i}function dT(n,e,t){let i=4,r=n.attrs,s=r!==null?pT(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!yi(i)&&!yi(c))return!1;if(o&&yi(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!uT(n,c,t)||c===""&&e.length===1){if(yi(i))return!1;o=!0}}else if(i&8){if(r===null||!lT(n,r,c,t)){if(yi(i))return!1;o=!0}}else{let l=e[++a],u=fT(c,r,Og(n),t);if(u===-1){if(yi(i))return!1;o=!0;continue}if(l!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(yi(i))return!1;o=!0}}}}return yi(i)||o}function yi(n){return(n&1)===0}function fT(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return mT(e,n)}function hT(n,e,t=!1){for(let i=0;i<e.length;i++)if(dT(n,e[i],t))return!0;return!1}function pT(n){for(let e=0;e<n.length;e++){let t=n[e];if(UC(t))return e}return n.length}function mT(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function rx(n,e){return n?":not("+e.trim()+")":e}function gT(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!yi(o)&&(e+=rx(s,r),r=""),i=o,s=s||!yi(i);t++}return r!==""&&(e+=rx(s,r)),e}function vT(n){return n.map(gT).join(",")}function yT(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!yi(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var kn={},$i=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})($i||{}),_T;function Fg(n,e){return _T(n,e)}var gV=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var lg=new WeakMap,lc=new WeakSet;function xT(n,e){let t=lg.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),lc.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function bT(n,e){let t=lg.get(n);t?t.includes(e)||t.push(e):lg.set(n,[e])}var Yr=new Set,Lg=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(Lg||{}),Go=new Ye(""),sx=new Set;function Jr(n){sx.has(n)||(sx.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var sb=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Ut({token:n,providedIn:"root",factory:()=>new n})}return n})();var kg=new Ye("",{factory:()=>{let n=et(ii),e=new Set;return n.onDestroy(()=>e.clear()),{queue:e,isScheduled:!1,scheduler:null,injector:n}}});function ob(n,e,t){let i=n.get(kg);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function ET(n,e){let t=n.get(kg);if(Array.isArray(e))for(let i of e)t.queue.delete(i);else t.queue.delete(e)}function ST(n,e){let t=n.get(kg);if(e.detachedLeaveAnimationFns){for(let i of e.detachedLeaveAnimationFns)t.queue.delete(i);e.detachedLeaveAnimationFns=void 0}}function MT(n,e){for(let[t,i]of e)ob(n,i.animateFns)}function ox(n,e,t,i){let r=n?.[Hi]?.enter;e!==null&&r&&r.has(t.index)&&MT(i,r)}function ax(n,e,t,i){try{t.get(tc)}catch(o){return i(!1)}let r=n?.[Hi];r?.enter?.has(e.index)&&ET(t,r.enter.get(e.index).animateFns);let s=wT(n,e,r);if(s.size===0){let o=!1;if(n){let a=[];Ed(n,e,a),o=a.length>0}if(!o)return i(!1)}n&&Yr.add(n[Vi]),ob(t,()=>CT(n,e,r||void 0,s,i),r||void 0)}function wT(n,e,t){let i=new Map,r=t?.leave;if(r&&r.has(e.index)&&i.set(e.index,r.get(e.index)),n&&r)for(let[s,o]of r){if(i.has(s))continue;let c=n[Ie].data[s].parent;for(;c;){if(c===e){i.set(s,o);break}c=c.parent}}return i}function CT(n,e,t,i,r){let s=[];if(t&&t.leave)for(let[o]of i){if(!t.leave.has(o))continue;let a=t.leave.get(o);for(let c of a.animateFns){let{promise:l}=c();s.push(l)}t.detachedLeaveAnimationFns=void 0}if(n&&Ed(n,e,s),s.length>0){let o=t||n?.[Hi];if(o){let a=o.running;a&&s.push(a),o.running=Promise.allSettled(s),DT(n,o.running,r)}else Promise.allSettled(s).then(()=>{n&&Yr.delete(n[Vi]),r(!0)})}else n&&Yr.delete(n[Vi]),r(!1)}function Ed(n,e,t){if(e.type&12){let r=n[e.index];if(si(r))for(let s=Bt;s<r.length;s++){let o=r[s];o[Ie].type===2&&TT(o,t)}}let i=e.child;for(;i;)Ed(n,i,t),i=i.next}function TT(n,e){let t=n[Hi];if(t&&t.leave)for(let r of t.leave.values())for(let s of r.animateFns){let{promise:o}=s();e.push(o)}let i=n[Ie].firstChild;for(;i;)Ed(n,i,e),i=i.next}function DT(n,e,t){e.then(()=>{n[Hi]?.running===e&&(n[Hi].running=void 0,Yr.delete(n[Vi])),t(!0)})}function Lo(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;si(r)?c=r:pr(r)&&(l=!0,r=r[vi]);let u=oi(r);n===0&&i!==null?(ox(a,i,s,t),o==null?tb(e,i,u):ud(e,i,u,o||null,!0)):n===1&&i!==null?(ox(a,i,s,t),ud(e,i,u,o||null,!0),xT(s,u)):n===2?(a?.[Hi]?.leave?.has(s.index)&&bT(s,u),lc.delete(u),ax(a,s,t,d=>{if(lc.has(u)){lc.delete(u);return}nb(e,u,l,d)})):n===3&&(lc.delete(u),ax(a,s,t,()=>{e.destroyNode(u)})),c!=null&&VT(e,n,t,c,s,i,o)}}function AT(n,e){ab(n,e),e[vi]=null,e[qn]=null}function IT(n,e,t,i,r,s){i[vi]=r,i[qn]=e,Md(n,i,t,1,r,s)}function ab(n,e){e[Ui].changeDetectionScheduler?.notify(9),Md(n,e,e[Nt],2,null,null)}function RT(n){let e=n[Ao];if(!e)return $m(n[Ie],n);for(;e;){let t=null;if(pr(e))t=e[Ao];else{let i=e[Bt];i&&(t=i)}if(!t){for(;e&&!e[ri]&&e!==n;)pr(e)&&$m(e[Ie],e),e=e[Qt];e===null&&(e=n),pr(e)&&$m(e[Ie],e),t=e&&e[ri]}e=t}}function Ug(n,e){let t=n[zs],i=t.indexOf(e);t.splice(i,1)}function Sd(n,e){if(Gs(e))return;let t=e[Nt];t.destroyNode&&Md(n,e,t,3,null,null),RT(e)}function $m(n,e){if(Gs(e))return;let t=Ne(null);try{e[Ue]&=-129,e[Ue]|=256,e[Xn]&&mo(e[Xn]),PT(n,e),NT(n,e),e[Ie].type===1&&e[Nt].destroy();let i=e[Gr];if(i!==null&&si(e[Qt])){i!==e[Qt]&&Ug(i,e);let r=e[Bi];r!==null&&r.detachView(n)}sg(e)}finally{Ne(t)}}function NT(n,e){let t=n.cleanup,i=e[Do];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[Do]=null);let r=e[ur];if(r!==null){e[ur]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[Br];if(s!==null){e[Br]=null;for(let o of s)o.destroy()}}function PT(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof Xs)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];Et(ft.LifecycleHookStart,a,c);try{c.call(a)}finally{Et(ft.LifecycleHookEnd,a,c)}}else{Et(ft.LifecycleHookStart,r,s);try{s.call(r)}finally{Et(ft.LifecycleHookEnd,r,s)}}}}}function OT(n,e,t){return FT(n,e.parent,t)}function FT(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[vi];if(mr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===_i.None||r===_i.Emulated)return null}return ai(i,t)}function LT(n,e,t){return UT(n,e,t)}function kT(n,e,t){return n.type&40?ai(n,t):null}var UT=kT,cx;function Bg(n,e,t,i){let r=OT(n,i,e),s=e[Nt],o=i.parent||e[qn],a=LT(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)ix(s,r,t[c],a,!1);else ix(s,r,t,a,!1);cx!==void 0&&cx(s,i,e,t,r)}function uc(n,e){if(e!==null){let t=e.type;if(t&3)return ai(e,n);if(t&4)return ug(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return uc(n,i);{let r=n[e.index];return si(r)?ug(-1,r):oi(r)}}else{if(t&128)return uc(n,e.next);if(t&32)return Fg(e,n)()||oi(n[e.index]);{let i=cb(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=dr(n[Ln]);return uc(r,i)}else return uc(n,e.next)}}}return null}function cb(n,e){if(e!==null){let i=n[Ln][qn],r=e.projection;return i.projection[r]}return null}function ug(n,e){let t=Bt+n+1;if(t<e.length){let i=e[t],r=i[Ie].firstChild;if(r!==null)return uc(i,r)}return e[jr]}function Vg(n,e,t,i,r,s,o){for(;t!=null;){let a=i[ki];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&Bo(oi(c),i),t.flags|=2),!Pg(t))if(l&8)Vg(n,e,t.child,i,r,s,!1),Lo(e,n,a,r,c,t,s,i);else if(l&32){let u=Fg(t,i),d;for(;d=u();)Lo(e,n,a,r,d,t,s,i);Lo(e,n,a,r,c,t,s,i)}else l&16?BT(n,e,i,t,r,s):Lo(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function Md(n,e,t,i,r,s){Vg(t,i,n.firstChild,e,r,s,!1)}function BT(n,e,t,i,r,s){let o=t[Ln],c=o[qn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];Lo(e,n,t[ki],r,u,i,s,t)}else{let l=c,u=o[Qt];Vx(i)&&(l.flags|=128),Vg(n,e,l,u,r,s,!0)}}function VT(n,e,t,i,r,s,o){let a=i[jr],c=oi(i);a!==c&&Lo(e,n,t,s,a,r,o);for(let l=Bt;l<i.length;l++){let u=i[l];Md(u[Ie],u,n,e,s,a)}}function HT(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:$i.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=$i.Important),n.setStyle(t,i,r,s))}}function Hg(n,e,t,i,r,s,o,a,c,l,u){let d=en+i,f=d+r,h=zT(d,f),g=typeof l=="function"?l():l;return h[Ie]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function zT(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:kn);return t}function GT(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Hg(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function zg(n,e,t,i,r,s,o,a,c,l,u){let d=e.blueprint.slice();return d[vi]=r,d[Ue]=i|4|128|8|64|1024,(l!==null||n&&n[Ue]&2048)&&(d[Ue]|=2048),Mm(d),d[Qt]=d[Vs]=n,d[zt]=t,d[Ui]=o||n&&n[Ui],d[Nt]=a||n&&n[Nt],d[ki]=c||n&&n[ki]||null,d[qn]=s,d[Vi]=JC(),d[To]=u,d[ym]=l,d[Ln]=e.type==2?n[Ln]:d,d}function jT(n,e,t){let i=ai(e,n),r=GT(t),s=n[Ui].rendererFactory,o=Gg(n,zg(n,r,null,lb(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function lb(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function ub(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Gg(n,e){return n[Ao]?n[vm][ri]=e:n[Ao]=e,n[vm]=e,e}function Ce(n=1){db(Gt(),Ge(),ji()+n,!1)}function db(n,e,t,i){if(!i)if((e[Ue]&3)===3){let s=n.preOrderCheckHooks;s!==null&&ed(e,s,t)}else{let s=n.preOrderHooks;s!==null&&td(e,s,0,t)}$r(t)}var wd=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(wd||{});function Ys(n,e,t,i){let r=Ne(null);try{let[s,o,a]=n.inputs[t],c=null;(o&wd.SignalBased)!==0&&(c=e[s][Fn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):Tx(e,c,s,i)}finally{Ne(r)}}function fb(n,e,t,i,r){let s=ji(),o=i&2;try{$r(-1),o&&e.length>en&&db(n,e,en,!1);let a=o?ft.TemplateUpdateStart:ft.TemplateCreateStart;Et(a,r,t),t(i,r)}finally{$r(s);let a=o?ft.TemplateUpdateEnd:ft.TemplateCreateEnd;Et(a,r,t)}}function hb(n,e,t){ZT(n,e,t),(t.flags&64)===64&&KT(n,e,t)}function jg(n,e,t=ai){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function WT(n,e,t,i){let s=i.get(Yx,Xx)||t===_i.ShadowDom||t===_i.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);if(o.tagName.toLowerCase()==="script")throw new ze(905,!1);return $T(o),o}function $T(n){qT(n)}var qT=()=>null;function XT(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function pb(n,e,t,i,r,s){let o=e[Ie];if(Wg(n,o,e,t,i)){mr(n)&&YT(e,n.index);return}n.type&3&&(t=XT(t)),mb(n,e,t,i,r,s)}function mb(n,e,t,i,r,s){if(n.type&3){let o=ai(n,e);i=s!=null?s(i,n.value||"",t):i,r.setProperty(o,t,i)}else n.type&12}function YT(n,e){let t=ci(e,n);t[Ue]&16||(t[Ue]|=64)}function ZT(n,e,t){let i=t.directiveStart,r=t.directiveEnd;mr(t)&&jT(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||cd(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=fc(e,n,o,t);if(Bo(c,e),s!==null&&nD(e,o-i,c,a,t,s),zi(a)){let l=ci(t.index,e);l[zt]=fc(e,n,o,t)}}}function KT(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=F0();try{$r(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Bu(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&JT(c,l)}}finally{$r(-1),Bu(o)}}function JT(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function QT(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];hT(e,s.selectors,!1)&&(i??=[],zi(s)?i.unshift(s):i.push(s))}return i}function eD(n,e,t,i,r,s){let o=ai(n,e);tD(e[Nt],o,s,n.value,t,i,r)}function tD(n,e,t,i,r,s,o){if(s==null)o?.(s,i||"",r),n.removeAttribute(e,r,t);else{let a=o==null?hr(s):o(s,i||"",r);n.setAttribute(e,r,a,t)}}function nD(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];Ys(i,t,c,l)}}function gb(n,e,t,i,r){let s=en+t,o=e[Ie],a=r(o,e,n,i,t);e[s]=a,No(n,!0);let c=n.type===2;return c?(ib(e[Nt],a,n),(w0()===0||Pu(n))&&Bo(a,e),C0()):Bo(a,e),ju()&&(!c||!Pg(n))&&Bg(o,e,a,n),n}function vb(n){let e=n;return Nm()?I0():(e=e.parent,No(e,!1)),e}function iD(n,e){let t=n[ki];if(!t)return;let i;try{i=t.get(qr,null)}catch(r){i=null}i?.(e)}function Wg(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],d=e.data[l];Ys(d,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];Ys(u,l,i,r),a=!0}return a}function rD(n,e,t,i,r,s){let o=null,a=null,c=null,l=!1,u=n.directiveToIndex.get(i.type);if(typeof u=="number"?o=u:[o,a,c]=u,a!==null&&c!==null&&n.hostDirectiveInputs?.hasOwnProperty(r)){let d=n.hostDirectiveInputs[r];for(let f=0;f<d.length;f+=2){let h=d[f];if(h>=a&&h<=c){let g=e.data[h],y=d[f+1];Ys(g,t[h],y,s),l=!0}else if(h>c)break}}return o!==null&&i.inputs.hasOwnProperty(r)&&(Ys(i,t[o],r,s),l=!0),l}function sD(n,e){let t=ci(e,n),i=t[Ie];oD(i,t);let r=t[vi];r!==null&&t[To]===null&&(t[To]=Zx(r,t[ki])),Et(ft.ComponentStart);try{$g(i,t,t[zt])}finally{Et(ft.ComponentEnd,t[zt])}}function oD(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function $g(n,e,t){Hu(e);try{let i=n.viewQuery;i!==null&&ag(1,i,t);let r=n.template;r!==null&&fb(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[Bi]?.finishViewCreation(n),n.staticContentQueries&&Kx(n,e),n.staticViewQueries&&ag(2,n.viewQuery,t);let s=n.components;s!==null&&aD(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Ue]&=-5,zu()}}function aD(n,e){for(let t=0;t<e.length;t++)sD(n,e[t])}function Cd(n,e,t,i){let r=Ne(null);try{let s=e.tView,a=n[Ue]&4096?4096:16,c=zg(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[Gr]=l;let u=n[Bi];return u!==null&&(c[Bi]=u.createEmbeddedView(s)),$g(s,c,t),c}finally{Ne(r)}}function hc(n,e){return!e||e.firstChild===null||Vx(n)}function pc(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(oi(s)),si(s)&&yb(s,i);let o=t.type;if(o&8)pc(n,e,t.child,i);else if(o&32){let a=Fg(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=cb(e,t);if(Array.isArray(a))i.push(...a);else{let c=dr(e[Ln]);pc(c[Ie],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function yb(n,e){for(let t=Bt;t<n.length;t++){let i=n[t],r=i[Ie].firstChild;r!==null&&pc(i[Ie],i,r,e)}n[jr]!==n[vi]&&e.push(n[jr])}function _b(n){if(n[Nu]!==null){for(let e of n[Nu])e.impl.addSequence(e);n[Nu].length=0}}var xb=[];function cD(n){return n[Xn]??lD(n)}function lD(n){let e=xb.pop()??Object.create(dD);return e.lView=n,e}function uD(n){n.lView[Xn]!==n&&(n.lView=null,xb.push(n))}var dD=ke(Ae({},Cs),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Ro(n.lView)},consumerOnSignalRead(){this.lView[Xn]=this}});function fD(n){let e=n[Xn]??Object.create(hD);return e.lView=n,e}var hD=ke(Ae({},Cs),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=dr(n.lView);for(;e&&!bb(e[Ie]);)e=dr(e);e&&wm(e)},consumerOnSignalRead(){this.lView[Xn]=this}});function bb(n){return n.type!==2}function Eb(n){if(n[Br]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Br])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Ue]&8192)}}var pD=100;function Sb(n,e=0){let i=n[Ui].rendererFactory,r=!1;r||i.begin?.();try{mD(n,e)}finally{r||i.end?.()}}function mD(n,e){let t=Pm();try{Ka(!0),dg(n,e);let i=0;for(;sc(n);){if(i===pD)throw new ze(103,!1);i++,dg(n,1)}}finally{Ka(t)}}function gD(n,e,t,i){if(Gs(e))return;let r=e[Ue],s=!1,o=!1;Hu(e);let a=!0,c=null,l=null;s||(bb(n)?(l=cD(e),c=po(l)):jl()===null?(a=!1,l=fD(e),c=po(l)):e[Xn]&&(mo(e[Xn]),e[Xn]=null));try{Mm(e),N0(n.bindingStartIndex),t!==null&&fb(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&ed(e,h,null)}else{let h=n.preOrderHooks;h!==null&&td(e,h,0,null),jm(e,0)}if(o||vD(e),Eb(e),Mb(e,0),n.contentQueries!==null&&Kx(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&ed(e,h)}else{let h=n.contentHooks;h!==null&&td(e,h,1),jm(e,1)}_D(n,e);let d=n.components;d!==null&&Cb(e,d,0);let f=n.viewQuery;if(f!==null&&ag(2,f,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&ed(e,h)}else{let h=n.viewHooks;h!==null&&td(e,h,2),jm(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Ru]){for(let h of e[Ru])h();e[Ru]=null}s||(_b(e),e[Ue]&=-73)}catch(u){throw s||Ro(e),u}finally{l!==null&&(qa(l,c),a&&uD(l)),zu()}}function Mb(n,e){for(let t=zx(n);t!==null;t=Gx(t))for(let i=Bt;i<t.length;i++){let r=t[i];wb(r,e)}}function vD(n){for(let e=zx(n);e!==null;e=Gx(e)){if(!(e[Ue]&2))continue;let t=e[zs];for(let i=0;i<t.length;i++){let r=t[i];wm(r)}}}function yD(n,e,t){Et(ft.ComponentStart);let i=ci(e,n);try{wb(i,t)}finally{Et(ft.ComponentEnd,i[zt])}}function wb(n,e){Fu(n)&&dg(n,e)}function dg(n,e){let i=n[Ie],r=n[Ue],s=n[Xn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&ql(s)),o||=!1,s&&(s.dirty=!1),n[Ue]&=-9217,o)gD(i,n,i.template,n[zt]);else if(r&8192){let a=Ne(null);try{Eb(n),Mb(n,1);let c=i.components;c!==null&&Cb(n,c,1),_b(n)}finally{Ne(a)}}}function Cb(n,e,t){for(let i=0;i<e.length;i++)yD(n,e[i],t)}function _D(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)$r(~r);else{let s=r,o=t[++i],a=t[++i];O0(o,s);let c=e[s];Et(ft.HostBindingsUpdateStart,c);try{a(2,c)}finally{Et(ft.HostBindingsUpdateEnd,c)}}}}finally{$r(-1)}}function qg(n,e){let t=Pm()?64:1088;for(n[Ui].changeDetectionScheduler?.notify(e);n;){n[Ue]|=t;let i=dr(n);if(Io(n)&&!i)return n;n=i}return null}function Tb(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function Db(n,e){let t=Bt+e;if(t<n.length)return n[t]}function Td(n,e,t,i=!0){let r=e[Ie];if(xD(r,e,n,t),i){let o=ug(t,n),a=e[Nt],c=a.parentNode(n[jr]);c!==null&&IT(r,n[qn],a,e,c,o)}let s=e[To];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function Ab(n,e){let t=mc(n,e);return t!==void 0&&Sd(t[Ie],t),t}function mc(n,e){if(n.length<=Bt)return;let t=Bt+e,i=n[t];if(i){let r=i[Gr];r!==null&&r!==n&&Ug(r,i),e>0&&(n[t-1][ri]=i[ri]);let s=ec(n,Bt+e);AT(i[Ie],i);let o=s[Bi];o!==null&&o.detachView(s[Ie]),i[Qt]=null,i[ri]=null,i[Ue]&=-129}return i}function xD(n,e,t,i){let r=Bt+i,s=t.length;i>0&&(t[r-1][ri]=e),i<s-Bt?(e[ri]=t[r],dm(t,Bt+i,e)):(t.push(e),e[ri]=null),e[Qt]=t;let o=e[Gr];o!==null&&t!==o&&Ib(o,e);let a=e[Bi];a!==null&&a.insertView(n),Lu(e),e[Ue]|=128}function Ib(n,e){let t=n[zs],i=e[Qt];if(pr(i))n[Ue]|=2;else{let r=i[Qt][Ln];e[Ln]!==r&&(n[Ue]|=2)}t===null?n[zs]=[e]:t.push(e)}var Zr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Ie];return pc(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[zt]}set context(e){this._lView[zt]=e}get destroyed(){return Gs(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Qt];if(si(e)){let t=e[rc],i=t?t.indexOf(this):-1;i>-1&&(mc(e,i),ec(t,i))}this._attachedToViewContainer=!1}Sd(this._lView[Ie],this._lView)}onDestroy(e){ku(this._lView,e)}markForCheck(){qg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Ue]&=-129}reattach(){Lu(this._lView),this._lView[Ue]|=128}detectChanges(){this._lView[Ue]|=1024,Sb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new ze(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Io(this._lView),t=this._lView[Gr];t!==null&&!e&&Ug(t,this._lView),ab(this._lView[Ie],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new ze(902,!1);this._appRef=e;let t=Io(this._lView),i=this._lView[Gr];i!==null&&!t&&Ib(i,this._lView),Lu(this._lView)}};var Vo=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=bD;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=Cd(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Zr(s)}}return n})();function bD(){return Xg(un(),Ge())}function Xg(n,e){return n.type&4?new Vo(e,n,zo(n,e)):null}function Dd(n,e,t,i,r){let s=n.data[e];if(s===null)s=ED(n,e,t,i,r),P0()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=A0();s.injectorIndex=o===null?-1:o.injectorIndex}return No(s,!0),s}function ED(n,e,t,i,r){let s=Rm(),o=Nm(),a=o?s:s&&s.parent,c=n.data[e]=MD(n,a,t,e,i,r);return SD(n,c,s,o),c}function SD(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function MD(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return D0()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Um(),attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function wD(n){let e=n[_m]??[],i=n[Qt][Nt],r=[];for(let s of e)s.data[qx]!==void 0?r.push(s):CD(s,i);n[_m]=r}function CD(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[$x];for(;t<r;){let s=i.nextSibling;nb(e,i,!1),i=s,t++}}}var TD=()=>null,DD=()=>null;function fg(n,e){return TD(n,e)}function Rb(n,e,t){return DD(n,e,t)}var Nb=class{},Zs=class{},jo=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>AD()}return n})();function AD(){let n=Ge(),e=un(),t=ci(e.index,n);return(pr(t)?t:n)[Nt]}var Pb=(()=>{class n{static \u0275prov=Ut({token:n,providedIn:"root",factory:()=>null})}return n})();function Ob(n){return n.debugInfo?.className||n.type.name||null}var id={},dd=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,id,i);return r!==id||t===id?r:this.parentInjector.get(e,t,i)}};function ID(n,e,t){return n[e]=t}function qi(n,e,t){if(t===kn)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function fd(n,e,t,i){let r=qi(n,e,t);return qi(n,e+1,i)||r}function RD(n,e,t,i,r,s){let o=fd(n,e,t,i);return fd(n,e+2,r,s)||o}function qs(n,e,t){return function i(r){let s=i.__ngNativeEl__;s!==void 0&&eT(r,s);let o=mr(n)?ci(n.index,e):e;qg(o,5);let a=e[zt],c=lx(e,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=lx(e,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function lx(n,e,t,i){let r=Ne(null);try{return Et(ft.OutputStart,e,t),t(i)!==!1}catch(s){return iD(n,s),!1}finally{Et(ft.OutputEnd,e,t),Ne(r)}}function Yg(n,e,t,i,r,s,o,a){let c=Pu(n),l=!1,u=null;if(!i&&c&&(u=PD(e,t,s,n.index)),u!==null){let d=u.__ngLastListenerFn__||u;d.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let d=ai(n,t),f=i?i(d):d;nT(t,f,s,a),i||(a.__ngNativeEl__=d);let h=r.listen(f,s,a);if(!ND(s)){let g=i?y=>i(oi(y[n.index])):n.index;Fb(g,e,t,s,a,h,!1)}}return l}function ND(n){return n.startsWith("animation")||n.startsWith("transition")}function PD(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Do],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function Fb(n,e,t,i,r,s,o){let a=e.firstCreatePass?Tm(e):null,c=Cm(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}function ux(n,e,t,i,r){let s=null,o=null,a=null,c=!1,l=n.directiveToIndex.get(t.type);if(typeof l=="number"?s=l:[s,o,a]=l,o!==null&&a!==null&&n.hostDirectiveOutputs?.hasOwnProperty(i)){let u=n.hostDirectiveOutputs[i];for(let d=0;d<u.length;d+=2){let f=u[d];if(f>=o&&f<=a)c=!0,hd(n,e,f,u[d+1],i,r);else if(f>a)break}}return t.outputs.hasOwnProperty(i)&&(c=!0,hd(n,e,s,i,i,r)),c}function hd(n,e,t,i,r,s){let o=e[t],a=e[Ie],l=a.data[t].outputs[i],d=o[l].subscribe(s);Fb(n.index,a,e,r,s,d,!0)}function Ad(){OD()}function OD(){let n=Ge(),e=Gt(),t=un();if(e.firstCreatePass&&LD(e,t),t.controlDirectiveIndex===-1)return;Jr("NgSignalForms");let i=n[t.controlDirectiveIndex];e.data[t.controlDirectiveIndex].controlDef.create(i,new pd(n,e,t))}function Id(){FD()}function FD(){let n=Ge(),e=Gt(),t=Po();if(t.controlDirectiveIndex===-1)return;let i=e.data[t.controlDirectiveIndex].controlDef,r=n[t.controlDirectiveIndex];i.update(r,new pd(n,e,t))}var pd=class{lView;tView;tNode;hasPassThrough;constructor(e,t,i){this.lView=e,this.tView=t,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return ai(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(e,t){let i=this.tView.data[this.tNode.customControlIndex];ux(this.tNode,this.lView,i,e,qs(this.tNode,this.lView,t))}listenToCustomControlModel(e){let t=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];ux(this.tNode,this.lView,i,t,qs(this.tNode,this.lView,e))}listenToDom(e,t){Yg(this.tNode,this.tView,this.lView,void 0,this.lView[Nt],e,t,qs(this.tNode,this.lView,t))}setInputOnDirectives(e,t){let i=this.tNode.inputs?.[e],r=this.tNode.hostDirectiveInputs?.[e];if(!i&&!r)return!1;let s=!1;if(i)for(let o of i){if(o===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[o],c=this.lView[o];Ys(a,c,e,t),s=!0}if(r)for(let o=0;o<r.length;o+=2){let a=r[o];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[o+1],l=this.tView.data[a],u=this.lView[a];Ys(l,u,c,t),s=!0}return s}setCustomControlModelInput(e){let t=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";rD(this.tNode,this.tView,this.lView,t,i,e)}customControlHasInput(e){if(this.tNode.customControlIndex===-1)return!1;let t=this.tView.data[this.tNode.customControlIndex];return(t.signalFormsInputPresence??=this._buildCustomControlInputCache(t))[e]===!0}_buildCustomControlInputCache(e){let t={};for(let i in e.inputs)t[i]=!0;if(e.hostDirectives!==null){let i=[...e.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let o in r.inputs)t[r.inputs[o]]=!0;let s=dx(r.directive);s!==null&&i.push(...s);continue}for(let s of r()){if(typeof s=="function")continue;if(s.inputs)for(let a=0;a<s.inputs.length;a+=2){let c=s.inputs[a+1]||s.inputs[a];t[c]=!0}let o=dx(s.directive);o!==null&&i.push(...o)}}}return t}};function dx(n){return typeof n=="function"&&"\u0275dir"in n?n.\u0275dir.hostDirectives??null:null}function LD(n,e,t){for(let r=e.directiveStart;r<e.directiveEnd;r++)if(n.data[r].controlDef){e.controlDirectiveIndex=r;break}if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(e.inputs?.[i.passThroughInput]?.length??0)>1){e.flags|=4096;return}kD(n,e)}function kD(n,e){for(let t=e.directiveStart;t<e.directiveEnd;t++){let i=n.data[t];if(!(e.directiveToIndex&&!e.directiveToIndex.has(i.type))){if(fx(i,"value")){e.flags|=1024,e.customControlIndex=t;return}if(fx(i,"checked")){e.flags|=2048,e.customControlIndex=t;return}}}if(e.hostDirectiveInputs!==null&&e.hostDirectiveOutputs!==null&&e.directiveToIndex!==null){let t=(i,r)=>{let s=e.hostDirectiveInputs[i],o=e.hostDirectiveOutputs[i+"Change"];if(!s||!o)return!1;for(let a=0;a<s.length;a+=2){let c=s[a];for(let l=0;l<o.length;l+=2){let u=o[l];if(c===u)for(let d of e.directiveToIndex.values()){if(!Array.isArray(d))continue;let[f,h,g]=d;if(c>=h&&c<=g)return e.flags|=r,e.customControlIndex=f,!0}}}return!1};if(t("value",1024)||t("checked",2048))return}}function fx(n,e){return UD(n,e)&&BD(n,e+"Change")}function UD(n,e){return e in n.inputs}function BD(n,e){return e in n.outputs}var hg=Symbol("BINDING");var Js=new Ye("");function md(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=tm(r,a);else if(s==2){let c=a,l=e[++o];i=tm(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function ct(n,e=0){let t=Ge();if(t===null)return mt(n,e);let i=un();return kx(i,t,ln(n),e)}function VD(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}GD(n,e,t,a,s,c,l)}s!==null&&i!==null&&HD(t,i,s)}function HD(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new ze(-301,!1);i.push(e[r],s)}}function zD(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function GD(n,e,t,i,r,s,o){let a=i.length,c=null;for(let f=0;f<a;f++){let h=i[f];c===null&&zi(h)&&(c=h,zD(n,t,f)),ig(cd(t,e),n,h.type)}YD(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let f=0;f<a;f++){let h=i[f];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,d=ub(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let f=0;f<a;f++){let h=i[f];if(t.mergedAttrs=Uo(t.mergedAttrs,h.hostAttrs),WD(n,t,e,d,h),XD(d,h,r),o!==null&&o.has(h)){let[y,p]=o.get(h);t.directiveToIndex.set(h.type,[d,y+t.directiveStart,p+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,d);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}jD(n,t,s)}function jD(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))hx(0,e,r,i),hx(1,e,r,i),mx(e,i,!1);else{let s=t.get(r);px(0,e,s,i),px(1,e,s,i),mx(e,i,!0)}}}function hx(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),Lb(e,s)}}function px(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),Lb(e,o)}}function Lb(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function mx(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||Og(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function WD(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Ur(r.type,!0)),o=new Xs(s,zi(r),ct,null);n.blueprint[i]=o,t[i]=o,$D(n,e,i,ub(n,t,r.hostVars,kn),r)}function $D(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;qD(o)!=a&&o.push(a),o.push(t,i,s)}}function qD(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function XD(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;zi(e)&&(t[""]=n)}}function YD(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function kb(n,e,t,i,r,s,o,a){let c=e[Ie],l=c.consts,u=Gi(l,o),d=Dd(c,n,t,i,u);return s&&VD(c,e,d,Gi(l,a),r),d.mergedAttrs=Uo(d.mergedAttrs,d.attrs),d.attrs!==null&&md(d,d.attrs,!1),d.mergedAttrs!==null&&md(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function Ub(n,e){PC(n,e),xm(e)&&n.queries.elementEnd(e)}function ZD(n,e,t,i,r,s){let o=e.consts,a=Gi(o,r),c=Dd(e,n,t,i,a);if(c.mergedAttrs=Uo(c.mergedAttrs,c.attrs),s!=null){let l=Gi(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&md(c,c.attrs,!1),c.mergedAttrs!==null&&md(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}var Bb=typeof ShadowRoot<"u",KD=typeof Document<"u";function JD(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&wd.SignalBased)!==0};return r&&(s.transform=r),s})}function QD(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function eA(n,e,t){let i=e instanceof ii?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new dd(t,i):t}function tA(n){let e=n.get(Zs,null);if(e===null)throw new ze(407,!1);let t=n.get(Pb,null),i=n.get(Vr,null),r=n.get(Go,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function nA(n,e){let t=Vb(n);return eb(e,t,t==="svg"?bm:t==="math"?y0:null)}function Vb(n){return(n.selectors[0][0]||"div").toLowerCase()}var gc=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=JD(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=QD(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=vT(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){Et(ft.DynamicComponentStart);let a=Ne(null);try{let c=this.componentDef,l=eA(c,r||this.ngModule,e),u=tA(l),d=u.tracingService;return d&&d.componentCreate?d.componentCreate(Ob(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{Ne(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=iA(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?WT(l,r,a.encapsulation,t):nA(a,l),d=t.get(Js,null),f=rA(u,()=>t.get(li,null)??Wx());d&&d.addHost(f);let h=o?.some(gx)||s?.some(p=>typeof p!="function"&&p.bindings.some(gx)),g=zg(null,c,null,512|lb(a),null,null,e,l,t,null,Zx(u,t,!0));d&&Bb&&f instanceof ShadowRoot&&ku(g,()=>{d.removeHost(f)}),g[en]=u,Hu(g);let y=null;try{let p=kb(en,g,2,"#host",()=>c.directiveRegistry,!0,0);ib(l,u,p),Bo(u,g),hb(c,g,p),Jx(c,p,g),Ub(c,p),i!==void 0&&oA(p,this.ngContentSelectors,i),y=ci(p.index,g),g[zt]=y[zt],$g(c,g,null)}catch(p){throw y!==null&&sg(y),sg(g),p}finally{Et(ft.DynamicComponentEnd),zu()}return new gd(this.componentType,g,!!h)}};function iA(n,e,t,i){let r=n?["ng-version","22.0.1"]:yT(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[hg].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let d=i[u];if(typeof d!="function")for(let f of d.bindings){a+=f[hg].requiredVars;let h=u+1;f.create&&(f.targetIdx=h,(s??=[]).push(f)),f.update&&(f.targetIdx=h,(o??=[]).push(f))}}let c=[e];if(i)for(let u of i){let d=typeof u=="function"?u:u.type,f=am(d);c.push(f)}return Hg(0,null,sA(s,o),1,a,c,null,null,null,[r],null)}function rA(n,e){let t=n.getRootNode?.();return KD&&t instanceof Document?t.head:t&&Bb&&t instanceof ShadowRoot?t:e().head}function sA(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function gx(n){let e=n[hg].kind;return e==="input"||e==="twoWay"}var gd=class extends Nb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Ou(t[Ie],en),this.location=zo(this._tNode,t),this.instance=ci(this._tNode.index,t)[zt],this.hostView=this.changeDetectorRef=new Zr(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Wg(i,r[Ie],r,e,t);this.previousInputValues.set(e,t);let o=ci(i.index,r);qg(o,1)}get injector(){return new Xr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function oA(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var bc=(()=>{class n{static __NG_ELEMENT_ID__=aA}return n})();function aA(){let n=un();return Hb(n,Ge())}var pg=class n extends bc{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return zo(this._hostTNode,this._hostLView)}get injector(){return new Xr(this._hostTNode,this._hostLView)}get parentInjector(){let e=Ig(this._hostTNode,this._hostLView);if(Rx(e)){let t=od(e,this._hostLView),i=sd(e),r=t[Ie].data[i+8];return new Xr(r,t)}else return new Xr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=vx(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-Bt}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=fg(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,hc(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c,l=t||{};c=l.index,i=l.injector,r=l.projectableNodes,s=l.environmentInjector||l.ngModuleRef,o=l.directives,a=l.bindings;let u=new gc(ks(e)),d=i||this.parentInjector;if(!s&&u.ngModule==null){let m=this.parentInjector.get(ii,null);m&&(s=m)}let f=ks(u.componentType??{}),h=fg(this._lContainer,f?.id??null),g=h?.firstChild??null,y=u.create(d,r,g,s,o,a);return this.insertImpl(y.hostView,c,hc(this._hostTNode,h)),y}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(b0(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[Qt],l=new n(c,c[qn],c[Qt]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return Td(o,r,s,i),e.attachToViewContainerRef(),dm(qm(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=vx(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=mc(this._lContainer,t);i&&(ec(qm(this._lContainer),t),Sd(i[Ie],i))}detach(e){let t=this._adjustIndex(e,-1),i=mc(this._lContainer,t);return i&&ec(qm(this._lContainer),t)!=null?new Zr(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function vx(n){return n[rc]}function qm(n){return n[rc]||(n[rc]=[])}function Hb(n,e){let t,i=e[n.index];return si(i)?t=i:(t=Tb(i,e,null,n),e[n.index]=t,Gg(e,t)),lA(t,e,n,i),new pg(t,n,e)}function cA(n,e){let t=n[Nt],i=t.createComment(""),r=ai(e,n),s=t.parentNode(r);return ud(t,s,i,t.nextSibling(r),!1),i}var lA=fA,uA=()=>!1;function dA(n,e,t){return uA(n,e,t)}function fA(n,e,t,i){if(n[jr])return;let r;t.type&8?r=oi(i):r=cA(e,t),n[jr]=r}var mg=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},gg=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Zg(e,t).matches!==null&&this.queries[t].setDirty()}},vg=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=xA(e):this.predicate=e}},yg=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},_g=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,hA(t,s)),this.matchTNodeWithReadOption(e,t,nd(t,e,s,!1,!1))}else i===Vo?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,nd(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===vr||r===bc||r===Vo&&t.type&4)this.addMatch(t.index,-2);else{let s=nd(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function hA(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function pA(n,e){return n.type&11?zo(n,e):n.type&4?Xg(n,e):null}function mA(n,e,t,i){return t===-1?pA(e,n):t===-2?gA(n,e,i):fc(n,n[Ie],t,e)}function gA(n,e,t){if(t===vr)return zo(e,n);if(t===Vo)return Xg(e,n);if(t===bc)return Hb(e,n)}function zb(n,e,t,i){let r=e[Bi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(mA(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function xg(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=zb(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let d=Bt;d<u.length;d++){let f=u[d];f[Gr]===f[Qt]&&xg(f[Ie],f,l,i)}if(u[zs]!==null){let d=u[zs];for(let f=0;f<d.length;f++){let h=d[f];xg(h[Ie],h,l,i)}}}}}return i}function vA(n,e){return n[Bi].queries[e].queryList}function yA(n,e,t){let i=new ld((t&4)===4);return M0(n,e,i,i.destroy),(e[Bi]??=new gg).queries.push(new mg(i))-1}function _A(n,e,t){let i=Gt();return i.firstCreatePass&&(bA(i,new vg(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),yA(i,Ge(),e)}function xA(n){return n.split(",").map(e=>e.trim())}function bA(n,e,t){n.queries===null&&(n.queries=new yg),n.queries.track(new _g(e,t))}function Zg(n,e){return n.queries.getByIndex(e)}function EA(n,e){let t=n[Ie],i=Zg(t,e);return i.crossesNgTemplate?xg(t,n,e,[]):zb(t,n,i,e)}function Wo(n){return!!n&&typeof n.then=="function"}function Kg(n){return!!n&&typeof n.subscribe=="function"}var vc=class{};var yc=class extends vc{injector;instance=null;constructor(e){super();let t=new Os([...e.providers,{provide:vc,useValue:this}],e.parent||ic(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function Gb(n,e,t=null){return new yc({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var SA=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=pm(!1,t.type),r=i.length>0?Gb([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Ut({token:n,providedIn:"environment",factory:()=>new n(mt(ii))})}return n})();function yr(n){return xc(()=>{let e=jb(n),t=ke(Ae({},e),{type:n.type,decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection!==Rg.Eager,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(SA).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||_i.Emulated,styles:n.styles||$n,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&Jr("NgStandalone"),Wb(t);let i=n.dependencies;return t.directiveDefs=yx(i,MA),t.pipeDefs=yx(i,r0),t.id=TA(t),t})}function MA(n){return ks(n)||am(n)}function $o(n){return xc(()=>({type:n.type,bootstrap:n.bootstrap||$n,declarations:n.declarations||$n,imports:n.imports||$n,exports:n.exports||$n,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function wA(n,e){if(n==null)return zr;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=wd.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function CA(n){if(n==null)return zr;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function Xi(n){return xc(()=>{let e=jb(n);return Wb(e),e})}function Jg(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function jb(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||zr,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||$n,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:wA(n.inputs,e),outputs:CA(n.outputs),debugInfo:null}}function Wb(n){n.features?.forEach(e=>e(n))}function yx(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function TA(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}var $b=new Ye("");var Qg=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=et($b,{optional:!0})??[];injector=et(gi);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=Iu(this.injector,r);if(Wo(s))t.push(s);else if(Kg(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Kr({token:n,factory:n.\u0275fac})}return n})();function ev(n){return e=>{e.controlDef={create:(t,i)=>{t?.\u0275ngControlCreate(i)},update:(t,i)=>{t?.\u0275ngControlUpdate?.(i)},passThroughInput:n}}}function DA(n){return Object.getPrototypeOf(n.prototype).constructor}function Qr(n){let e=DA(n.type),t=!0,i=[n];for(;e;){let r;if(zi(n))r=e.\u0275cmp||e.\u0275dir;else{if(e.\u0275cmp)throw new ze(903,!1);r=e.\u0275dir}if(r){if(t){i.push(r);let o=n;o.inputs=Xm(n.inputs),o.declaredInputs=Xm(n.declaredInputs),o.outputs=Xm(n.outputs);let a=r.hostBindings;a&&PA(n,a);let c=r.viewQuery,l=r.contentQueries;if(c&&RA(n,c),l&&NA(n,l),AA(n,r),i0(n.outputs,r.outputs),zi(r)&&r.data.animation){let u=n.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let o=0;o<s.length;o++){let a=s[o];a&&a.ngInherit&&a(n),a===Qr&&(t=!1)}}e=Object.getPrototypeOf(e)}IA(i)}function AA(n,e){for(let t in e.inputs){if(!e.inputs.hasOwnProperty(t)||n.inputs.hasOwnProperty(t))continue;let i=e.inputs[t];i!==void 0&&(n.inputs[t]=i,n.declaredInputs[t]=e.declaredInputs[t])}}function IA(n){let e=0,t=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=e+=r.hostVars,r.hostAttrs=Uo(r.hostAttrs,t=Uo(t,r.hostAttrs))}}function Xm(n){return n===zr?{}:n===$n?[]:n}function RA(n,e){let t=n.viewQuery;t?n.viewQuery=(i,r)=>{e(i,r),t(i,r)}:n.viewQuery=e}function NA(n,e){let t=n.contentQueries;t?n.contentQueries=(i,r,s)=>{e(i,r,s),t(i,r,s)}:n.contentQueries=e}function PA(n,e){let t=n.hostBindings;t?n.hostBindings=(i,r)=>{e(i,r),t(i,r)}:n.hostBindings=e}function OA(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=Uo(n.mergedAttrs,n.attrs);let u=n.tView=Hg(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),No(n,!1);let c=FA(t,e,n,i);ju()&&Bg(t,e,c,n),Bo(c,e);let l=Tb(c,e,c,n);e[i+en]=l,Gg(e,l),dA(l,n,e)}function vd(n,e,t,i,r,s,o,a,c,l,u){let d=t+en,f;if(e.firstCreatePass){if(f=Dd(e,d,4,o||null,a||null),l!=null){let h=Gi(e.consts,l);f.localNames=[];for(let g=0;g<h.length;g+=2)f.localNames.push(h[g],-1)}}else f=e.data[d];return OA(f,n,e,t,i,r,s,c),l!=null&&jg(n,f,u),f}var FA=LA;function LA(n,e,t,i){return Wu(!0),e[Nt].createComment("")}var tv=new Ye("");var qb=new Ye("");function Xb(){Tp(()=>{let n="";throw new ze(600,n)})}var kA=10;var qo=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=et(qr);afterRenderManager=et(sb);zonelessEnabled=et(Fo);rootEffectScheduler=et(Zu);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new mi;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=et(Ws);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Is(t=>!t))}constructor(){et(Go,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=et(ii);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=gi.NULL){return this._injector.get(Rt).run(()=>{if(Et(ft.BootstrapComponentStart),!this._injector.get(Qg).done){let m="";throw new ze(405,m)}let a=ks(t),c=this._injector.get(vc),l=new gc(a,c);this.componentTypes.push(t);let{hostElement:u,directives:d,bindings:f}=UA(i),h=u||l.selector,g=l.create(r,[],h,c.injector,d,f),y=g.location.nativeElement,p=g.injector.get(tv,null);return p?.registerApplication(y),g.onDestroy(()=>{this.detachView(g.hostView),dc(this.components,g),p?.unregisterApplication(y)}),this._loadComponent(g),Et(ft.BootstrapComponentEnd,g),g})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Et(ft.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Lg.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Et(ft.ChangeDetectionEnd),new ze(101,!1);let t=Ne(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Ne(t),this.afterTick.next(),Et(ft.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Zs,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<kA;){Et(ft.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Et(ft.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!sc(r))continue;let s=i&&!this.zonelessEnabled?0:1;Sb(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>sc(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;dc(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(qb,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>dc(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new ze(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Kr({token:n,factory:n.\u0275fac})}return n})();function UA(n){return n===void 0||typeof n=="string"||n instanceof Element?{hostElement:n}:n}function dc(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function xn(n,e,t,i){let r=Ge(),s=Wr();if(qi(r,s,e)){let o=Gt(),a=Po();eD(a,r,n,e,t,i)}return xn}function nv(){return Ge()[Ln][zt]}var bg=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function Ym(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function BA(n,e,t,i){let r,s,o=0,a=n.length-1,c=void 0;if(Array.isArray(e)){Ne(i);let l=e.length-1;for(Ne(null);o<=a&&o<=l;){let u=n.at(o),d=e[o],f=Ym(o,u,o,d,t);if(f!==0){f<0&&n.updateValue(o,d),o++;continue}let h=n.at(a),g=e[l],y=Ym(a,h,l,g,t);if(y!==0){y<0&&n.updateValue(a,g),a--,l--;continue}let p=t(o,u),m=t(a,h),b=t(o,d);if(Object.is(b,m)){let M=t(l,g);Object.is(M,p)?(n.swap(o,a),n.updateValue(a,g),l--,a--):n.move(a,o),n.updateValue(o,d),o++;continue}if(r??=new yd,s??=xx(n,o,a,t),Eg(n,r,o,b))n.updateValue(o,d),o++,a++;else if(s.has(b))r.set(p,n.detach(o)),a--;else{let M=n.create(o,e[o]);n.attach(o,M),o++,a++}}for(;o<=l;)_x(n,r,t,o,e[o]),o++}else if(e!=null){Ne(i);let l=e[Symbol.iterator]();Ne(null);let u=l.next();for(;!u.done&&o<=a;){let d=n.at(o),f=u.value,h=Ym(o,d,o,f,t);if(h!==0)h<0&&n.updateValue(o,f),o++,u=l.next();else{r??=new yd,s??=xx(n,o,a,t);let g=t(o,f);if(Eg(n,r,o,g))n.updateValue(o,f),o++,a++,u=l.next();else if(!s.has(g))n.attach(o,n.create(o,f)),o++,a++,u=l.next();else{let y=t(o,d);r.set(y,n.detach(o)),a--}}}for(;!u.done;)_x(n,r,t,n.length,u.value),u=l.next()}for(;o<=a;)n.destroy(n.detach(a--));r?.forEach(l=>{n.destroy(l)})}function Eg(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function _x(n,e,t,i,r){if(Eg(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function xx(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var yd=class{kvMap=new Map;_vMap=void 0;has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function Un(n,e,t,i,r,s,o,a){Jr("NgControlFlow");let c=Ge(),l=Gt(),u=Gi(l.consts,s);return vd(c,l,n,e,t,i,r,u,256,o,a),iv}function iv(n,e,t,i,r,s,o,a){Jr("NgControlFlow");let c=Ge(),l=Gt(),u=Gi(l.consts,s);return vd(c,l,n,e,t,i,r,u,512,o,a),iv}function Bn(n,e){Jr("NgControlFlow");let t=Ge(),i=Wr(),r=t[i]!==kn?t[i]:-1,s=r!==-1?_d(t,en+r):void 0,o=0;if(qi(t,i,n)){let a=Ne(null);try{if(s!==void 0&&Ab(s,o),n!==-1){let c=en+n,l=_d(t,c),u=Cg(t[Ie],c),d=Rb(l,u,t),f=Cd(t,u,e,{dehydratedView:d});Td(l,f,o,hc(u,d))}}finally{Ne(a)}}else if(s!==void 0){let a=Db(s,o);a!==void 0&&(a[zt]=e)}}var Sg=class{lContainer;$implicit;$index;constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Bt}};function Rd(n){return n}var Mg=class{hasEmptyBlock;trackByFn;liveCollection;constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function _r(n,e,t,i,r,s,o,a,c,l,u,d,f){Jr("NgControlFlow");let h=Ge(),g=Gt(),y=c!==void 0,p=Ge(),m=a?o.bind(p[Ln][zt]):o,b=new Mg(y,m);p[en+n]=b,vd(h,g,n+1,e,t,i,r,Gi(g.consts,s),256),y&&vd(h,g,n+2,c,l,u,d,Gi(g.consts,f),512)}var wg=class extends bg{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-Bt}at(e){return this.getLView(e)[zt].$implicit}attach(e,t){let i=t[To];this.needsIndexUpdate||=e!==this.length,Td(this.lContainer,t,e,hc(this.templateTNode,i)),VA(this.lContainer,e)}detach(e){return this.needsIndexUpdate||=e!==this.length-1,HA(this.lContainer,e),zA(this.lContainer,e)}create(e,t){let i=fg(this.lContainer,this.templateTNode.tView.ssrId);return Cd(this.hostLView,this.templateTNode,new Sg(this.lContainer,t,e),{dehydratedView:i})}destroy(e){Sd(e[Ie],e)}updateValue(e,t){this.getLView(e)[zt].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[zt].$index=e}getLView(e){return GA(this.lContainer,e)}};function xr(n){let e=Ne(null),t=ji();try{let i=Ge(),r=i[Ie],s=i[t],o=t+1,a=_d(i,o);if(s.liveCollection===void 0){let l=Cg(r,o);s.liveCollection=new wg(a,i,l)}else s.liveCollection.reset();let c=s.liveCollection;if(BA(c,n,s.trackByFn,e),c.updateIndexes(),s.hasEmptyBlock){let l=Wr(),u=c.length===0;if(qi(i,l,u)){let d=t+2,f=_d(i,d);if(u){let h=Cg(r,d),g=Rb(f,h,i),y=Cd(i,h,void 0,{dehydratedView:g});Td(f,y,0,hc(h,g))}else r.firstUpdatePass&&wD(f),Ab(f,0)}}}finally{Ne(e)}}function _d(n,e){return n[e]}function VA(n,e){if(n.length<=Bt)return;let t=Bt+e,i=n[t],r=i?i[Hi]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let s=i[ki];ST(s,r),Yr.delete(i[Vi]),r.detachedLeaveAnimationFns=void 0}}function HA(n,e){if(n.length<=Bt)return;let t=Bt+e,i=n[t],r=i?i[Hi]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function zA(n,e){return mc(n,e)}function GA(n,e){return Db(n,e)}function Cg(n,e){return Ou(n,e)}function bn(n,e,t){let i=Ge(),r=Wr();if(qi(i,r,e)){let s=Gt(),o=Po();pb(o,i,n,e,i[Nt],t)}return bn}function bx(n,e,t,i,r){Wg(e,n,t,r?"class":"style",i)}function pe(n,e,t,i){let r=Ge(),s=r[Ie],o=n+en,a=s.firstCreatePass?kb(o,r,2,e,QT,T0(),t,i):s.data[o];if(mr(a)){let c=r[Ui].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(Ob(l),()=>(Ex(n,e,r,a,i),pe))}}return Ex(n,e,r,a,i),pe}function Ex(n,e,t,i,r){if(gb(i,t,n,e,Yb),Pu(i)){let s=t[Ie];hb(s,t,i),Jx(s,i,t)}r!=null&&jg(t,i)}function ge(){let n=Gt(),e=un(),t=vb(e);return n.firstCreatePass&&Ub(n,t),Am(t)&&Im(),Dm(),t.classesWithoutHost!=null&&FC(t)&&bx(n,t,Ge(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&LC(t)&&bx(n,t,Ge(),t.stylesWithoutHost,!1),ge}function Ec(n,e,t,i){return pe(n,e,t,i),ge(),Ec}function En(n,e,t,i){let r=Ge(),s=r[Ie],o=n+en,a=s.firstCreatePass?ZD(o,s,2,e,t,i):s.data[o];return gb(a,r,n,e,Yb),i!=null&&jg(r,a),En}function Sn(){let n=un(),e=vb(n);return Am(e)&&Im(),Dm(),Sn}var Yb=(n,e,t,i,r)=>(Wu(!0),eb(e[Nt],i,Um()));function br(){return Ge()}function Xo(n,e,t){let i=Ge(),r=Wr();if(qi(i,r,e)){let s=Gt(),o=Po();mb(o,i,n,e,i[Nt],t)}return Xo}var cc=void 0;function jA(n){let e=Math.floor(Math.abs(n)),t=n.toString().replace(/^[^.]*\.?/,"").length;return e===1&&t===0?1:5}var WA=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],cc,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],cc,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",cc,cc,cc],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",jA],Zm=Object.create(null);function Nd(n){let e=$A(n),t=Sx(e);if(t)return t;let i=e.split("-")[0];if(t=Sx(i),t)return t;if(i==="en")return WA;throw new ze(701,!1)}function Sx(n){return n in Zm||(Zm[n]=Us.ng&&Us.ng.common&&Us.ng.common.locales&&Us.ng.common.locales[n]),Zm[n]}var Yo={LocaleId:0,DayPeriodsFormat:1,DayPeriodsStandalone:2,DaysFormat:3,DaysStandalone:4,MonthsFormat:5,MonthsStandalone:6,Eras:7,FirstDayOfWeek:8,WeekendRange:9,DateFormat:10,TimeFormat:11,DateTimeFormat:12,NumberSymbols:13,NumberFormats:14,CurrencyCode:15,CurrencySymbol:16,CurrencyName:17,Currencies:18,Directionality:19,PluralCase:20,ExtraData:21};function $A(n){return n.toLowerCase().replace(/_/g,"-")}var Sc="en-US";var qA=Sc;function Zb(n){typeof n=="string"&&(qA=n.toLowerCase().replace(/_/g,"-"))}function Pt(n,e,t){let i=Ge(),r=Gt(),s=un();return Kb(r,i,i[Nt],s,n,e,t),Pt}function xi(n,e,t){let i=Ge(),r=Gt(),s=un();return(s.type&3||t)&&Yg(s,r,i,t,i[Nt],n,e,qs(s,i,e)),xi}function Kb(n,e,t,i,r,s,o){let a=!0,c=null;if((i.type&3||o)&&(c??=qs(i,e,s),Yg(i,n,e,o,t,r,s,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let d=0;d<u.length;d+=2){let f=u[d],h=u[d+1];c??=qs(i,e,s),hd(i,e,f,h,r,c)}if(l&&l.length)for(let d of l)c??=qs(i,e,s),hd(i,e,d,r,r,c)}}function nt(n=1){return V0(n)}function Pd(n,e,t){return _A(n,e,t),Pd}function rv(n){let e=Ge(),t=Gt(),i=Fm();Vu(i+1);let r=Zg(t,i);if(n.dirty&&x0(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=EA(e,i);n.reset(s,YC),n.notifyOnChanges()}return!0}return!1}function sv(){return vA(Ge(),Fm())}function Qu(n,e){return n<<17|e<<2}function Ks(n){return n>>17&32767}function XA(n){return(n&2)==2}function YA(n,e){return n&131071|e<<17}function Tg(n){return n|2}function Ho(n){return(n&131068)>>2}function Km(n,e){return n&-131069|e<<2}function ZA(n){return(n&1)===1}function Dg(n){return n|1}function KA(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=Ks(o),c=Ho(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||Co(d,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let f=Ks(n[a+1]);n[i+1]=Qu(f,a),f!==0&&(n[f+1]=Km(n[f+1],i)),n[a+1]=YA(n[a+1],i)}else n[i+1]=Qu(a,0),a!==0&&(n[a+1]=Km(n[a+1],i)),a=i;else n[i+1]=Qu(c,0),a===0?a=i:n[c+1]=Km(n[c+1],i),c=i;l&&(n[i+1]=Tg(n[i+1])),Mx(n,u,i,!0),Mx(n,u,i,!1),JA(e,u,n,i,s),o=Qu(a,c),s?e.classBindings=o:e.styleBindings=o}function JA(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Co(s,e)>=0&&(t[i+1]=Dg(t[i+1]))}function Mx(n,e,t,i){let r=n[t+1],s=e===null,o=i?Ks(r):Ho(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];QA(c,e)&&(a=!0,n[o+1]=i?Dg(l):Tg(l)),o=i?Ks(l):Ho(l)}a&&(n[t+1]=i?Tg(r):Dg(r))}function QA(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Co(n,e)>=0:!1}function Od(n,e,t){return Jb(n,e,t,!1),Od}function dn(n,e){return Jb(n,e,null,!0),dn}function Jb(n,e,t,i){let r=Ge(),s=Gt(),o=Uu(2);if(s.firstUpdatePass&&tI(s,n,o,i),e!==kn&&qi(r,o,e)){let a=s.data[ji()];oI(s,a,r,r[Nt],n,r[o+1]=aI(e,t),i,o)}}function eI(n,e){return e>=n.expandoStartIndex}function tI(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[ji()],o=eI(n,t);cI(s,i)&&e===null&&!o&&(e=!1),e=nI(r,s,e,i),KA(r,s,e,t,o,i)}}function nI(n,e,t,i){let r=L0(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=Jm(null,n,e,t,i),t=_c(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=Jm(r,n,e,t,i),s===null){let c=iI(n,e,i);c!==void 0&&Array.isArray(c)&&(c=Jm(null,n,e,c[1],i),c=_c(c,e.attrs,i),rI(n,e,i,c))}else s=sI(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function iI(n,e,t){let i=t?e.classBindings:e.styleBindings;if(Ho(i)!==0)return n[Ks(i)]}function rI(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[Ks(r)]=i}function sI(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=_c(i,o,t)}return _c(i,e.attrs,t)}function Jm(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=_c(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function _c(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),d0(n,o,t?!0:e[++s]))}return n===void 0?null:n}function oI(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=ZA(l)?wx(c,e,t,r,Ho(l),o):void 0;if(!xd(u)){xd(s)||XA(l)&&(s=wx(c,null,t,r,a,o));let d=Em(ji(),t);HT(i,o,d,r,s)}}function wx(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,d=u===null,f=t[r+1];f===kn&&(f=d?$n:void 0);let h=d?Au(f,i):u===i?f:void 0;if(l&&!xd(h)&&(h=Au(c,i)),xd(h)&&(a=h,o))return a;let g=n[r+1];r=o?Ks(g):Ho(g)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=Au(c,i))}return a}function xd(n){return n!==void 0}function aI(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=Mu(Qx(n)))),n}function cI(n,e){return(n.flags&(e?8:16))!==0}function Ee(n,e=""){let t=Ge(),i=Gt(),r=n+en,s=i.firstCreatePass?Dd(i,r,1,e,null):i.data[r],o=lI(i,t,s,e);t[r]=o,ju()&&Bg(i,t,o,s),No(s,!1)}var lI=(n,e,t,i)=>(Wu(!0),rT(e[Nt],i));function uI(n,e,t,i=""){return qi(n,Wr(),t)?e+hr(t)+i:kn}function dI(n,e,t,i,r,s=""){let o=Om(),a=fd(n,o,t,r);return Uu(2),a?e+hr(t)+i+hr(r)+s:kn}function fI(n,e,t,i,r,s,o,a,c,l=""){let u=Om(),d=RD(n,u,t,r,o,c);return Uu(4),d?e+hr(t)+i+hr(r)+s+hr(o)+a+hr(c)+l:kn}function fn(n){return Er("",n),fn}function Er(n,e,t){let i=Ge(),r=uI(i,n,e,t);return r!==kn&&ov(i,ji(),r),Er}function Fd(n,e,t,i,r){let s=Ge(),o=dI(s,n,e,t,i,r);return o!==kn&&ov(s,ji(),o),Fd}function Ld(n,e,t,i,r,s,o,a,c){let l=Ge(),u=fI(l,n,e,t,i,r,s,o,a,c);return u!==kn&&ov(l,ji(),u),Ld}function ov(n,e,t){let i=Em(e,n);sT(n[Nt],i,t)}function Mc(n,e,t){Ju(e)&&(e=e());let i=Ge(),r=Wr();if(qi(i,r,e)){let s=Gt(),o=Po();pb(o,i,n,e,i[Nt],t)}return Mc}function kd(n,e){let t=Ju(n);return t&&n.set(e),t}function wc(n,e){let t=Ge(),i=Gt(),r=un();return Kb(i,t,t[Nt],r,n,e),wc}function Cx(n,e,t){let i=Gt();i.firstCreatePass&&Qb(e,i.data,i.blueprint,zi(n),t)}function Qb(n,e,t,i,r){if(n=ln(n),Array.isArray(n))for(let s=0;s<n.length;s++)Qb(n[s],e,t,i,r);else{let s=Gt(),o=Ge(),a=un(),c=Ps(n)?n:ln(n.provide),l=gm(n),u=a.providerIndexes&1048575,d=a.directiveStart,f=a.providerIndexes>>20;if(Ps(n)||!n.multi){let h=new Xs(l,r,ct,null),g=eg(c,e,r?u:u+f,d);g===-1?(ig(cd(a,o),s,c),Qm(s,n,e.length),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(h),o.push(h)):(t[g]=h,o[g]=h)}else{let h=eg(c,e,u+f,d),g=eg(c,e,u,u+f),y=h>=0&&t[h],p=g>=0&&t[g];if(r&&!p||!r&&!y){ig(cd(a,o),s,c);let m=mI(r?pI:hI,t.length,r,i,l,n);!r&&p&&(t[g].providerFactory=m),Qm(s,n,e.length,0),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(m),o.push(m)}else{let m=eE(t[r?g:h],l,!r&&i);Qm(s,n,h>-1?h:g,m)}!r&&i&&p&&t[g].componentProviders++}}}function Qm(n,e,t,i){let r=Ps(e),s=m0(e);if(r||s){let c=(s?ln(e.useClass):e).prototype.ngOnDestroy;if(c){let l=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=l.indexOf(t);u===-1?l.push(t,[i,c]):l[u+1].push(i,c)}else l.push(t,c)}}}function eE(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function eg(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function hI(n,e,t,i,r){return Ag(this.multi,[])}function pI(n,e,t,i,r){let s=this.multi,o;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=fc(i,i[Ie],this.providerFactory.index,r);o=c.slice(0,a),Ag(s,o);for(let l=a;l<c.length;l++)o.push(c[l])}else o=[],Ag(s,o);return o}function Ag(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function mI(n,e,t,i,r,s){let o=new Xs(n,t,ct,null);return o.multi=[],o.index=e,o.componentProviders=0,eE(o,r,i&&!t),o}function Cc(n,e){return t=>{t.providersResolver=(i,r)=>Cx(i,r?r(n):n,!1),e&&(t.viewProvidersResolver=(i,r)=>Cx(i,r?r(e):e,!0))}}function gI(n,e){let t=n[e];return t===kn?void 0:t}function vI(n,e,t,i,r,s,o){let a=e+t;return fd(n,a,r,s)?ID(n,a+2,o?i.call(o,r,s):i(r,s)):gI(n,a+2)}function Zo(n,e){let t=Gt(),i,r=n+en;t.firstCreatePass?(i=yI(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=Ur(i.type,!0)),o,a=In(ct);try{let c=ad(!1),l=s();return ad(c),Sm(t,Ge(),r,l),l}finally{In(a)}}function yI(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function Ko(n,e,t,i){let r=n+en,s=Ge(),o=_0(s,r);return _I(s,r)?vI(s,R0(),e,o.transform,t,i,o):o.transform(t,i)}function _I(n,e){return n[Ie].data[e].pure}var tE=(()=>{class n{applicationErrorHandler=et(qr);appRef=et(qo);taskService=et(Ws);ngZone=et(Rt);zonelessEnabled=et(Fo);tracing=et(Go,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Ft;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ja):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(et(Yu,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?j0:Vm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ja+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Kr({token:n,factory:n.\u0275fac})}return n})();function nE(){return[{provide:Vr,useExisting:tE},{provide:Rt,useClass:Qa},{provide:Fo,useValue:!0}]}function xI(){return typeof $localize<"u"&&$localize.locale||Sc}var Tc=new Ye("",{factory:()=>et(Tc,{optional:!0,skipSelf:!0})||xI()});function Jo(n,e){return Zl(n,e?.equal)}function bi(n){return D_(n)}var OI=1e4;var rj=OI-1e3;var Yi=(()=>{class n{static __NG_ELEMENT_ID__=FI}return n})();function FI(n){return LI(un(),Ge(),(n&16)===16)}function LI(n,e,t){if(mr(n)&&!t){let i=ci(n.index,e);return new Zr(i,i)}else if(n.type&175){let i=e[Ln];return new Zr(i,e)}return null}var kI=(()=>{class n{zone=et(Rt);changeDetectionScheduler=et(Vr);applicationRef=et(qo);applicationErrorHandler=et(qr);_onMicrotaskEmptySubscription;initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{try{this.applicationRef.dirtyFlags|=1,this.applicationRef._tick()}catch(t){this.applicationErrorHandler(t)}})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Kr({token:n,factory:n.\u0275fac})}return n})(),UI=new Ye("",{factory:()=>!1});function BI({ngZoneFactory:n,scheduleInRootZone:e}){return n??=()=>new Rt(ke(Ae({},rE()),{scheduleInRootZone:e})),[{provide:Fo,useValue:!1},{provide:Rt,useFactory:n},{provide:Bs,multi:!0,useFactory:()=>{let t=et(kI,{optional:!0});return()=>t.initialize()}},{provide:Bs,multi:!0,useFactory:()=>{let t=et(VI);return()=>{t.initialize()}}},{provide:Yu,useValue:e??Bm}]}function iE(n){let e=n?.scheduleInRootZone,t=BI({ngZoneFactory:()=>{let i=rE(n);return i.scheduleInRootZone=e,i.shouldCoalesceEventChangeDetection&&Jr("NgZone_CoalesceEvent"),new Rt(i)},scheduleInRootZone:e});return hm([{provide:UI,useValue:!0},t])}function rE(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var VI=(()=>{class n{subscription=new Ft;initialized=!1;zone=et(Rt);pendingTasks=et(Ws);initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{Rt.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{Rt.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Kr({token:n,factory:n.\u0275fac})}return n})();var av=new Ye(""),HI=new Ye("");function Dc(n){return!n.moduleRef}function zI(n){let e=Dc(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Rt);return t.run(()=>{Dc(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(qr),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),Dc(n)){let s=()=>e.destroy(),o=n.platformInjector.get(av);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(av);o.add(s),n.moduleRef.onDestroy(()=>{dc(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return jI(i,t,()=>{let s=e.get(Ws),o=s.add(),a=e.get(Qg);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(Tc,Sc);if(Zb(c||Sc),!e.get(HI,!0))return Dc(n)?e.get(qo):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Dc(n)){let u=e.get(qo);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return GI?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var GI;function jI(n,e,t){try{let i=t();return Wo(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var Ud=null;function WI(n=[],e){return gi.create({name:e,providers:[{provide:nc,useValue:"platform"},{provide:av,useValue:new Set([()=>Ud=null])},...n]})}function $I(n=[]){if(Ud)return Ud;let e=WI(n);return Ud=e,Xb(),qI(e),e}function qI(n){let e=n.get($u,null);Iu(n,()=>{e?.forEach(t=>t())})}function sE(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;Et(ft.BootstrapApplicationStart);try{let s=r?.injector??$I(i),o=[nE(),$0,...t||[]],a=new yc({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return zI({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{Et(ft.BootstrapApplicationEnd)}}function cv(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}var oE=null;function es(){return oE}function lv(n){oE??=n}var Ac=class{};var fv=(function(n){return n[n.Decimal=0]="Decimal",n[n.Percent=1]="Percent",n[n.Currency=2]="Currency",n[n.Scientific=3]="Scientific",n})(fv||{});var Zi={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Qo(n,e){let t=Nd(n),i=t[Yo.NumberSymbols][e];if(typeof i>"u"){if(e===Zi.CurrencyDecimal)return t[Yo.NumberSymbols][Zi.Decimal];if(e===Zi.CurrencyGroup)return t[Yo.NumberSymbols][Zi.Group]}return i}function cE(n,e){return Nd(n)[Yo.NumberFormats][e]}var XI=/^(\d+)?\.((\d+)(-(\d+))?)?$/,aE=22,Vd=".",Ic="0",YI=";",ZI=",",uv="#";function KI(n,e,t,i,r,s,o=!1){let a="",c=!1;if(!isFinite(n))a=Qo(t,Zi.Infinity);else{let l=e1(n);o&&(l=QI(l));let u=e.minInt,d=e.minFrac,f=e.maxFrac;if(s){let b=s.match(XI);if(b===null)throw new ze(2306,!1);let M=b[1],S=b[3],A=b[5];M!=null&&(u=dv(M)),S!=null&&(d=dv(S)),A!=null?f=dv(A):S!=null&&d>f&&(f=d);let w=100;if(u>w||d>w||f>w)throw new ze(2306,!1)}t1(l,d,f);let h=l.digits,g=l.integerLen,y=l.exponent,p=[];for(c=h.every(b=>!b);g<u;g++)h.unshift(0);for(;g<0;g++)h.unshift(0);g>0?p=h.splice(g,h.length):(p=h,h=[0]);let m=[];for(h.length>=e.lgSize&&m.unshift(h.splice(-e.lgSize,h.length).join(""));h.length>e.gSize;)m.unshift(h.splice(-e.gSize,h.length).join(""));h.length&&m.unshift(h.join("")),a=m.join(Qo(t,i)),p.length&&(a+=Qo(t,r)+p.join("")),y&&(a+=Qo(t,Zi.Exponential)+"+"+y)}return n<0&&!c?a=e.negPre+a+e.negSuf:a=e.posPre+a+e.posSuf,a}function lE(n,e,t){let i=cE(e,fv.Decimal),r=JI(i,Qo(e,Zi.MinusSign));return KI(n,r,e,Zi.Group,Zi.Decimal,t)}function JI(n,e="-"){let t={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=n.split(YI),r=i[0],s=i[1],o=r.indexOf(Vd)!==-1?r.split(Vd):[r.substring(0,r.lastIndexOf(Ic)+1),r.substring(r.lastIndexOf(Ic)+1)],a=o[0],c=o[1]||"";t.posPre=a.substring(0,a.indexOf(uv));for(let u=0;u<c.length;u++){let d=c.charAt(u);d===Ic?t.minFrac=t.maxFrac=u+1:d===uv?t.maxFrac=u+1:t.posSuf+=d}let l=a.split(ZI);if(t.gSize=l[1]?l[1].length:0,t.lgSize=l[2]||l[1]?(l[2]||l[1]).length:0,s){let u=r.length-t.posPre.length-t.posSuf.length,d=s.indexOf(uv);t.negPre=s.substring(0,d).replace(/'/g,""),t.negSuf=s.slice(d+u).replace(/'/g,"")}else t.negPre=e+t.posPre,t.negSuf=t.posSuf;return t}function QI(n){if(n.digits[0]===0)return n;let e=n.digits.length-n.integerLen;return n.exponent?n.exponent+=2:(e===0?n.digits.push(0,0):e===1&&n.digits.push(0),n.integerLen+=2),n}function e1(n){let e=Math.abs(n)+"",t=0,i,r,s,o,a;for((r=e.indexOf(Vd))>-1&&(e=e.replace(Vd,"")),(s=e.search(/e/i))>0?(r<0&&(r=s),r+=+e.slice(s+1),e=e.substring(0,s)):r<0&&(r=e.length),s=0;e.charAt(s)===Ic;s++);if(s===(a=e.length))i=[0],r=1;else{for(a--;e.charAt(a)===Ic;)a--;for(r-=s,i=[],o=0;s<=a;s++,o++)i[o]=Number(e.charAt(s))}return r>aE&&(i=i.splice(0,aE-1),t=r-1,r=1),{digits:i,exponent:t,integerLen:r}}function t1(n,e,t){if(e>t)throw new ze(2307,!1);let i=n.digits,r=i.length-n.integerLen,s=Math.min(Math.max(e,r),t),o=s+n.integerLen,a=i[o];if(o>0){i.splice(Math.max(n.integerLen,o));for(let d=o;d<i.length;d++)i[d]=0}else{r=Math.max(0,r),n.integerLen=1,i.length=Math.max(1,o=s+1),i[0]=0;for(let d=1;d<o;d++)i[d]=0}if(a>=5)if(o-1<0){for(let d=0;d>o;d--)i.unshift(0),n.integerLen++;i.unshift(1),n.integerLen++}else i[o-1]++;for(;r<Math.max(0,s);r++)i.push(0);let c=s!==0,l=e+n.integerLen,u=i.reduceRight(function(d,f,h,g){return f=f+d,g[h]=f<10?f:f-10,c&&(g[h]===0&&h>=l?g.pop():c=!1),f>=10?1:0},0);u&&(i.unshift(u),n.integerLen++)}function dv(n){let e=parseInt(n);if(isNaN(e))throw new ze(2305,!1);return e}function n1(n,e){return new ze(2100,!1)}var hv=(()=>{class n{_locale;constructor(t){this._locale=t}transform(t,i,r){if(!i1(t))return null;r||=this._locale;try{let s=r1(t);return lE(s,r,i)}catch(s){throw n1(n,s.message)}}static \u0275fac=function(i){return new(i||n)(ct(Tc,16))};static \u0275pipe=Jg({name:"number",type:n,pure:!0})}return n})();function i1(n){return!(n==null||n===""||n!==n)}function r1(n){if(typeof n=="string"&&!isNaN(Number(n)-parseFloat(n)))return Number(n);if(typeof n!="number")throw new ze(2309,!1);return n}function pv(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var uE="browser";var Rc=class{_doc;constructor(e){this._doc=e}manager},Hd=(()=>{class n extends Rc{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(mt(li))};static \u0275prov=Ut({token:n,factory:n.\u0275fac})}return n})(),jd=new Ye(""),yv=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof Hd));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof Hd);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new ze(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(mt(jd),mt(Rt))};static \u0275prov=Ut({token:n,factory:n.\u0275fac})}return n})(),mv="ng-app-id";function dE(n){for(let e of n)e.remove()}function fE(n,e){let t=e.createElement("style");return t.textContent=n,t}function a1(n,e,t,i){let r=n.head?.querySelectorAll(`style[${mv}="${e}"],link[${mv}="${e}"]`);if(!r||r.length===0)return!1;for(let s of r)s.removeAttribute(mv),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]});return!0}function vv(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var _v=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,a1(t,i,this.inline,this.external)&&this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,fE);i?.forEach(r=>this.addUsage(r,this.external,vv))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(dE(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])dE(t);this.hosts.clear()}addHost(t){if(!this.hosts.has(t)){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,fE(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,vv(i,this.doc)))}}removeHost(t){this.hosts.delete(t);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let s of i.elements)s.parentNode===t?s.remove():r.push(s);i.elements=r}}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(mt(li),mt(oc),mt(qu,8),mt(ac))};static \u0275prov=Ut({token:n,factory:n.\u0275fac})}return n})(),gv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},xv=/%COMP%/g;var pE="%COMP%",c1=`_nghost-${pE}`,l1=`_ngcontent-${pE}`,u1=!0,d1=new Ye("",{factory:()=>u1});function f1(n){return l1.replace(xv,n)}function h1(n){return c1.replace(xv,n)}function mE(n,e){return e.map(t=>t.replace(xv,n))}var bv=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Nc(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Gd?r.applyToHost(t):r instanceof Pc&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(i.encapsulation){case _i.Emulated:s=new Gd(c,l,i,this.appId,u,o,a,d);break;case _i.ShadowDom:return new zd(c,t,i,o,a,this.nonce,d,l);case _i.ExperimentalIsolatedShadowDom:return new zd(c,t,i,o,a,this.nonce,d);default:s=new Pc(c,l,i,u,o,a,d);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(mt(yv),mt(Js),mt(oc),mt(d1),mt(li),mt(Rt),mt(qu),mt(Go,8))};static \u0275prov=Ut({token:n,factory:n.\u0275fac})}return n})(),Nc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(gv[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(hE(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(hE(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new ze(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=gv[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=gv[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&($i.DashCase|$i.Important)?e.style.setProperty(t,i,r&$i.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&$i.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=es().getGlobalEventTarget(this.doc,e),!e))throw new ze(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function hE(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var zd=class extends Nc{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=mE(i.id,l);for(let d of l){let f=document.createElement("style");o&&f.setAttribute("nonce",o),f.textContent=d,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let d of u){let f=vv(d,r);o&&f.setAttribute("nonce",o),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Pc=class extends Nc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?mE(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Yr.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Gd=class extends Pc{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=f1(l),this.hostAttr=h1(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var Wd=class n extends Ac{supportsDOMEvents=!0;static makeCurrent(){lv(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=p1();return t==null?null:m1(t)}resetBaseElement(){Oc=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return pv(document.cookie,e)}},Oc=null;function p1(){return Oc=Oc||document.head.querySelector("base"),Oc?Oc.getAttribute("href"):null}function m1(n){return new URL(n,document.baseURI).pathname}var gE=["alt","control","meta","shift"],g1={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},v1={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},vE=(()=>{class n extends Rc{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>es().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),gE.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=g1[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),gE.forEach(o=>{if(o!==r){let a=v1[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(mt(li))};static \u0275prov=Ut({token:n,factory:n.\u0275fac})}return n})();function Ev(n,e,t){return cn(this,null,function*(){let i=Ae({rootComponent:n},y1(e,t));return sE(i)})}function y1(n,e){return{platformRef:e?.platformRef,appProviders:[...S1,...n?.providers??[]],platformProviders:E1}}function _1(){Wd.makeCurrent()}function x1(){return new fr}function b1(){return Ng(document),document}var E1=[{provide:ac,useValue:uE},{provide:$u,useValue:_1,multi:!0},{provide:li,useFactory:b1}];var S1=[{provide:nc,useValue:"root"},{provide:fr,useFactory:x1},{provide:jd,useClass:Hd,multi:!0},{provide:jd,useClass:vE,multi:!0},bv,{provide:Js,useClass:_v},{provide:_v,useExisting:Js},yv,{provide:Zs,useExisting:bv},[]];var yE={providers:[iE({eventCoalescing:!0})]};var TE=(()=>{class n{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,i){this._renderer=t,this._elementRef=i}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(i){return new(i||n)(ct(jo),ct(vr))};static \u0275dir=Xi({type:n})}return n})(),M1=(()=>{class n extends TE{static \u0275fac=(()=>{let t;return function(r){return(t||(t=bd(n)))(r||n)}})();static \u0275dir=Xi({type:n,features:[Qr]})}return n})(),DE=new Ye("");var w1={provide:DE,useExisting:Hr(()=>Kd),multi:!0};function C1(){let n=es()?es().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var T1=new Ye(""),Kd=(()=>{class n extends TE{_compositionMode;_composing=!1;constructor(t,i,r){super(t,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!C1())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(i){return new(i||n)(ct(jo),ct(vr),ct(T1,8))};static \u0275dir=Xi({type:n,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&Pt("input",function(o){return r._handleInput(o.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(o){return r._compositionEnd(o.target.value)})},standalone:!1,features:[Cc([w1]),Qr]})}return n})();function Av(n){return n==null||Iv(n)===0}function Iv(n){return n==null?null:Array.isArray(n)||typeof n=="string"?n.length:n instanceof Set?n.size:null}var AE=new Ye(""),D1=new Ye(""),A1=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Sv=class{static min(e){return I1(e)}static max(e){return R1(e)}static required(e){return IE(e)}static requiredTrue(e){return N1(e)}static email(e){return P1(e)}static minLength(e){return O1(e)}static maxLength(e){return F1(e)}static pattern(e){return L1(e)}static nullValidator(e){return qd()}static compose(e){return LE(e)}static composeAsync(e){return UE(e)}};function I1(n){return e=>{if(e.value==null||n==null)return null;let t=parseFloat(e.value);return!isNaN(t)&&t<n?{min:{min:n,actual:e.value}}:null}}function R1(n){return e=>{if(e.value==null||n==null)return null;let t=parseFloat(e.value);return!isNaN(t)&&t>n?{max:{max:n,actual:e.value}}:null}}function IE(n){return Av(n.value)?{required:!0}:null}function N1(n){return n.value===!0?null:{required:!0}}function P1(n){return Av(n.value)||A1.test(n.value)?null:{email:!0}}function O1(n){return e=>{let t=e.value?.length??Iv(e.value);return t===null||t===0?null:t<n?{minlength:{requiredLength:n,actualLength:t}}:null}}function F1(n){return e=>{let t=e.value?.length??Iv(e.value);return t!==null&&t>n?{maxlength:{requiredLength:n,actualLength:t}}:null}}function L1(n){if(!n)return qd;let e,t;return typeof n=="string"?(t="",n.charAt(0)!=="^"&&(t+="^"),t+=n,n.charAt(n.length-1)!=="$"&&(t+="$"),e=new RegExp(t)):(t=n.toString(),e=n),i=>{if(Av(i.value))return null;let r=i.value;return e.test(r)?null:{pattern:{requiredPattern:t,actualValue:r}}}}function qd(n){return null}function RE(n){return n!=null}function NE(n){return Wo(n)?Vp(n):n}function PE(n){let e={};return n.forEach(t=>{e=t!=null?Ae(Ae({},e),t):e}),Object.keys(e).length===0?null:e}function OE(n,e){return e.map(t=>t(n))}function k1(n){return!n.validate}function FE(n){return n.map(e=>k1(e)?e:t=>e.validate(t))}function LE(n){if(!n)return null;let e=n.filter(RE);return e.length==0?null:function(t){return PE(OE(t,e))}}function kE(n){return n!=null?LE(FE(n)):null}function UE(n){if(!n)return null;let e=n.filter(RE);return e.length==0?null:function(t){let i=OE(t,e).map(NE);return Hp(i).pipe(Is(PE))}}function BE(n){return n!=null?UE(FE(n)):null}function _E(n,e){return n===null?[e]:Array.isArray(n)?[...n,e]:[n,e]}function U1(n){return n._rawValidators}function B1(n){return n._rawAsyncValidators}function Mv(n){return n?Array.isArray(n)?n:[n]:[]}function Xd(n,e){return Array.isArray(n)?n.includes(e):n===e}function xE(n,e){let t=Mv(e);return Mv(n).forEach(r=>{Xd(t,r)||t.push(r)}),t}function bE(n,e){return Mv(e).filter(t=>!Xd(n,t))}var Yd=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(e){this._rawValidators=e||[],this._composedValidatorFn=kE(this._rawValidators)}_setAsyncValidators(e){this._rawAsyncValidators=e||[],this._composedAsyncValidatorFn=BE(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(e){this._onDestroyCallbacks.push(e)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(e=>e()),this._onDestroyCallbacks=[]}reset(e=void 0){this.control?.reset(e)}hasError(e,t){return this.control?this.control.hasError(e,t):!1}getError(e,t){return this.control?this.control.getError(e,t):null}},wv=class extends Yd{name;get formDirective(){return null}get path(){return null}};var Fc="VALID",$d="INVALID",ea="PENDING",Lc="DISABLED",Qs=class{},Zd=class extends Qs{value;source;constructor(e,t){super(),this.value=e,this.source=t}},kc=class extends Qs{pristine;source;constructor(e,t){super(),this.pristine=e,this.source=t}},Uc=class extends Qs{touched;source;constructor(e,t){super(),this.touched=e,this.source=t}},ta=class extends Qs{status;source;constructor(e,t){super(),this.status=e,this.source=t}};var Bc=class extends Qs{source;constructor(e){super(),this.source=e}};function V1(n){return(Jd(n)?n.validators:n)||null}function H1(n){return Array.isArray(n)?kE(n):n||null}function z1(n,e){return(Jd(e)?e.asyncValidators:n)||null}function G1(n){return Array.isArray(n)?BE(n):n||null}function Jd(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}var Cv=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=$s(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(e,t){this._assignValidators(e),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(e){this._rawValidators=this._composedValidatorFn=e,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(e){this._rawAsyncValidators=this._composedAsyncValidatorFn=e}get parent(){return this._parent}get status(){return bi(this.statusReactive)}set status(e){bi(()=>this.statusReactive.set(e))}_status=Jo(()=>this.statusReactive());statusReactive=$s(void 0);get valid(){return this.status===Fc}get invalid(){return this.status===$d}get pending(){return this.status===ea}get disabled(){return this.status===Lc}get enabled(){return this.status!==Lc}errors;get pristine(){return bi(this.pristineReactive)}set pristine(e){bi(()=>this.pristineReactive.set(e))}_pristine=Jo(()=>this.pristineReactive());pristineReactive=$s(!0);get dirty(){return!this.pristine}get touched(){return bi(this.touchedReactive)}set touched(e){bi(()=>this.touchedReactive.set(e))}_touched=Jo(()=>this.touchedReactive());touchedReactive=$s(!1);get untouched(){return!this.touched}_events=new mi;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(e){this._assignValidators(e)}setAsyncValidators(e){this._assignAsyncValidators(e)}addValidators(e){this.setValidators(xE(e,this._rawValidators))}addAsyncValidators(e){this.setAsyncValidators(xE(e,this._rawAsyncValidators))}removeValidators(e){this.setValidators(bE(e,this._rawValidators))}removeAsyncValidators(e){this.setAsyncValidators(bE(e,this._rawAsyncValidators))}hasValidator(e){return Xd(this._rawValidators,e)}hasAsyncValidator(e){return Xd(this._rawAsyncValidators,e)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(e={}){let t=this.touched===!1;this.touched=!0;let i=e.sourceControl??this;e.onlySelf||this._parent?.markAsTouched(ke(Ae({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Uc(!0,i))}markAllAsDirty(e={}){this.markAsDirty({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsDirty(e))}markAllAsTouched(e={}){this.markAsTouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(e))}markAsUntouched(e={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:i})}),e.onlySelf||this._parent?._updateTouched(e,i),t&&e.emitEvent!==!1&&this._events.next(new Uc(!1,i))}markAsDirty(e={}){let t=this.pristine===!0;this.pristine=!1;let i=e.sourceControl??this;e.onlySelf||this._parent?.markAsDirty(ke(Ae({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new kc(!1,i))}markAsPristine(e={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:e.emitEvent})}),e.onlySelf||this._parent?._updatePristine(e,i),t&&e.emitEvent!==!1&&this._events.next(new kc(!0,i))}markAsPending(e={}){this.status=ea;let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new ta(this.status,t)),this.statusChanges.emit(this.status)),e.onlySelf||this._parent?.markAsPending(ke(Ae({},e),{sourceControl:t}))}disable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Lc,this.errors=null,this._forEachChild(r=>{r.disable(ke(Ae({},e),{onlySelf:!0}))}),this._updateValue();let i=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Zd(this.value,i)),this._events.next(new ta(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(ke(Ae({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Fc,this._forEachChild(i=>{i.enable(ke(Ae({},e),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent}),this._updateAncestors(ke(Ae({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(e,t){e.onlySelf||(this._parent?.updateValueAndValidity(e),e.skipPristineCheck||this._parent?._updatePristine({},t),this._parent?._updateTouched({},t))}setParent(e){this._parent=e}getRawValue(){return this.value}updateValueAndValidity(e={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Fc||this.status===ea)&&this._runAsyncValidator(i,e.emitEvent)}let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Zd(this.value,t)),this._events.next(new ta(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),e.onlySelf||this._parent?.updateValueAndValidity(ke(Ae({},e),{sourceControl:t}))}_updateTreeValidity(e={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(e)),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Lc:Fc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(e,t){if(this.asyncValidator){this.status=ea,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1,shouldHaveEmitted:e!==!1};let i=NE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:e})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let e=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,e}return!1}setErrors(e,t={}){this.errors=e,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(e){let t=e;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(e,t){let i=t?this.get(t):this;return i?.errors?i.errors[e]:null}hasError(e,t){return!!this.getError(e,t)}get root(){let e=this;for(;e._parent;)e=e._parent;return e}_updateControlsErrors(e,t,i){this.status=this._calculateStatus(),e&&this.statusChanges.emit(this.status),(e||i)&&this._events.next(new ta(this.status,t)),this._parent&&this._parent._updateControlsErrors(e,t,i)}_initObservables(){this.valueChanges=new xt,this.statusChanges=new xt}_calculateStatus(){return this._allControlsDisabled()?Lc:this.errors?$d:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ea)?ea:this._anyControlsHaveStatus($d)?$d:Fc}_anyControlsHaveStatus(e){return this._anyControls(t=>t.status===e)}_anyControlsDirty(){return this._anyControls(e=>e.dirty)}_anyControlsTouched(){return this._anyControls(e=>e.touched)}_updatePristine(e,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,e.onlySelf||this._parent?._updatePristine(e,t),r&&this._events.next(new kc(this.pristine,t))}_updateTouched(e={},t){this.touched=this._anyControlsTouched(),this._events.next(new Uc(this.touched,t)),e.onlySelf||this._parent?._updateTouched(e,t)}_onDisabledChange=[];_registerOnCollectionChange(e){this._onCollectionChange=e}_setUpdateStrategy(e){Jd(e)&&e.updateOn!=null&&(this._updateOn=e.updateOn)}_parentMarkedDirty(e){return!e&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(e){return null}_assignValidators(e){this._rawValidators=Array.isArray(e)?e.slice():e,this._composedValidatorFn=H1(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(e){this._rawAsyncValidators=Array.isArray(e)?e.slice():e,this._composedAsyncValidatorFn=G1(this._rawAsyncValidators)}_updateHasRequiredValidator(){bi(()=>this._hasRequired.set(this.hasValidator(Sv.required)))}};function j1(n){return n.tagName==="INPUT"||n.tagName==="SELECT"||n.tagName==="TEXTAREA"}function W1(n,e,t,i){switch(t){case"name":n.setAttribute(e,t,i);break;case"disabled":case"readonly":case"required":i?n.setAttribute(e,t,""):n.removeAttribute(e,t);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?n.setAttribute(e,t,i.toString()):n.removeAttribute(e,t);break}}var Tv=class{kind;context;control;message;constructor({kind:e,context:t,control:i}){this.kind=e,this.context=t,this.control=i}};var $1=(()=>{class n{_validator=qd;_onChange;_enabled;ngOnChanges(t){if(this.inputName in t){let i=this.normalizeInput(t[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):qd,this._onChange?.()}}validate(t){return this._validator(t)}registerOnValidatorChange(t){this._onChange=t}enabled(t){return t!=null}static \u0275fac=function(i){return new(i||n)};static \u0275dir=Xi({type:n,features:[gr]})}return n})();var q1={provide:AE,useExisting:Hr(()=>VE),multi:!0};var VE=(()=>{class n extends $1{required;inputName="required";normalizeInput=cv;createValidator=t=>IE;enabled(t){return t}static \u0275fac=(()=>{let t;return function(r){return(t||(t=bd(n)))(r||n)}})();static \u0275dir=Xi({type:n,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&xn("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Cc([q1]),Qr]})}return n})();var X1=new Ye(""),HE=new Ye("",{factory:()=>Rv}),Rv="always";function Y1(n,e){return[...e.path,n]}function EE(n,e,t=Rv){K1(n,e),e.valueAccessor.writeValue(n.value),(n.disabled||t==="always")&&e.valueAccessor.setDisabledState?.(n.disabled),J1(n,e),eR(n,e),Q1(n,e),Z1(n,e)}function SE(n,e){n.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(e)})}function Z1(n,e){if(e.valueAccessor.setDisabledState){let t=i=>{e.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(t),e._registerOnDestroy(()=>{n._unregisterOnDisabledChange(t)})}}function K1(n,e){let t=U1(n);e.validator!==null?n.setValidators(_E(t,e.validator)):typeof t=="function"&&n.setValidators([t]);let i=B1(n);e.asyncValidator!==null?n.setAsyncValidators(_E(i,e.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();SE(e._rawValidators,r),SE(e._rawAsyncValidators,r)}function J1(n,e){e.valueAccessor.registerOnChange(t=>{n._pendingValue=t,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&zE(n,e)})}function Q1(n,e){e.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&zE(n,e),n.updateOn!=="submit"&&n.markAsTouched()})}function zE(n,e){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),e.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function eR(n,e){let t=(i,r)=>{e.valueAccessor.writeValue(i),r&&e.viewToModelUpdate(i)};n.registerOnChange(t),e._registerOnDestroy(()=>{n._unregisterOnChange(t)})}function tR(n,e){if(!n.hasOwnProperty("model"))return!1;let t=n.model;return t.isFirstChange()?!0:!Object.is(e,t.currentValue)}function nR(n){return Object.getPrototypeOf(n.constructor)===M1}function iR(n,e){if(!e)return null;Array.isArray(e);let t,i,r;return e.forEach(s=>{s.constructor===Kd?t=s:nR(s)?i=s:r=s}),r||i||t||null}var rR={provide:X1,useFactory:()=>{let n=et(na,{self:!0});return{setParseErrors:e=>{n.setParseErrorSource(e)},set onReset(e){n.onReset=e}}}},na=class extends Yd{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(e){this.userOnReset=e,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(t=>{t instanceof Bc&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=iR(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(e,t,i){super(),this.injector=e,this.renderer=t,this.rawValueAccessors=i,this.injector?.get(js)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let e=this.injector?.get(Yi);if(!this.control||!e)return;let t=e.markForCheck.bind(e);this.subscription=new Ft,this.subscription.add(this.control.valueChanges.subscribe(t)),this.subscription.add(this.control.statusChanges.subscribe(t)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof Bc&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(e){!e.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!e.customControl||(this.isCustomControlBased=!0,e.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),e.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=j1(e.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof VE))}ngControlUpdate(e,t){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,e.setCustomControlModelInput(i.value)),this.bindControlProperty(e,r,"touched",i.touched),this.bindControlProperty(e,r,"dirty",i.dirty),this.bindControlProperty(e,r,"valid",i.valid),this.bindControlProperty(e,r,"invalid",i.invalid),this.bindControlProperty(e,r,"pending",i.pending),this.bindControlProperty(e,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(e,r,"required",this.isRequired);let s=i.errors;if(r.errors!==s){r.errors=s;let o=this._convertErrors(s);e.setInputOnDirectives("errors",o)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(e,t,i,r){if(t[i]===r)return;t[i]=r;let s=e.setInputOnDirectives(i,r);this.isNativeFormElement&&!s&&(i==="disabled"||i==="required")&&this.renderer&&W1(this.renderer,e.nativeElement,i,r)}_convertErrors(e){if(e===null)return[];let t=this.control;return Object.entries(e).map(([i,r])=>new Tv({context:r,kind:i,control:t}))}setParseErrorSource(e){if(e===void 0)return;let t=null,i=Jo(()=>{let r=e();return r.length===0?null:r.reduce((s,o)=>(s[o.kind]=o,s),{})});this.parseErrorsValidator=(()=>t).bind(this),Gm(()=>{t=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(e){this.parseErrorsValidator&&(e?.removeValidators(this.parseErrorsValidator),e?.updateValueAndValidity({emitEvent:!1}))}},Dv=class{_cd;constructor(e){this._cd=e}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var GE=(()=>{class n extends Dv{constructor(t){super(t)}static \u0275fac=function(i){return new(i||n)(ct(na,2))};static \u0275dir=Xi({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&dn("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Qr]})}return n})();function ME(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function wE(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var sR=class extends Cv{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(e=null,t,i){super(V1(t),z1(i,t)),this._applyFormState(e),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Jd(t)&&(t.nonNullable||t.initialValueIsDefault)&&(wE(e)?this.defaultValue=e.value:this.defaultValue=e)}setValue(e,t={}){bi(()=>{this.value=this._pendingValue=e,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)})}patchValue(e,t={}){this.setValue(e,t)}reset(e=this.defaultValue,t={}){this._applyFormState(e),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),t.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,t?.emitEvent!==!1&&this._events.next(new Bc(this))}_updateValue(){}_anyControls(e){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(e){this._onChange.push(e)}_unregisterOnChange(e){ME(this._onChange,e)}registerOnDisabledChange(e){this._onDisabledChange.push(e)}_unregisterOnDisabledChange(e){ME(this._onDisabledChange,e)}_forEachChild(e){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(e){wE(e)?(this.value=this._pendingValue=e.value,e.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=e}};var oR={provide:na,useExisting:Hr(()=>Nv)},CE=Promise.resolve(),Nv=(()=>{class n extends na{_changeDetectorRef;callSetDisabledState;control=new sR;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new xt;constructor(t,i,r,s,o,a,c,l){super(c,l,s),this._changeDetectorRef=o,this.callSetDisabledState=a,this._parent=t,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),tR(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(t){super.ngControlCreate(t)}\u0275ngControlUpdate(t){super.ngControlUpdate(t,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,EE(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(t){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,EE(this.control,this,t))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){CE.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&cv(i);CE.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?Y1(t,this._parent):[t]}static \u0275fac=function(i){return new(i||n)(ct(wv,9),ct(AE,10),ct(D1,10),ct(DE,10),ct(Yi,8),ct(HE,8),ct(gi,8),ct(jo,8))};static \u0275dir=Xi({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Cc([oR,rR]),Qr,gr,ev(null)]})}return n})();var aR=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=$o({type:n});static \u0275inj=Fs({})}return n})();var jE=(()=>{class n{static withConfig(t){return{ngModule:n,providers:[{provide:HE,useValue:t.callSetDisabledState??Rv}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=$o({type:n});static \u0275inj=Fs({imports:[aR]})}return n})();var Qd=class n{updateControlPoint(e,t,i,r){let s=e.controlPoints.map((o,a)=>o.map((c,l)=>a===t&&l===i?Ae({},r):Ae({},c)));return ke(Ae({},e),{controlPoints:s})}moveControlPoint(e,t,i,r,s,o){let a=e.controlPoints.map((c,l)=>c.map((u,d)=>l===t&&d===i?ke(Ae({},u),{x:r,y:s,z:o}):Ae({},u)));return ke(Ae({},e),{controlPoints:a})}setKnotsU(e,t){return ke(Ae({},e),{knotsU:[...t]})}setKnotsV(e,t){return ke(Ae({},e),{knotsV:[...t]})}validateKnotsForPatch(e,t,i){let r=e.controlPoints.length,s=e.controlPoints[0]?.length??0;return{u:this.validateKnots(t,e.degreeU,r,"U"),v:this.validateKnots(i,e.degreeV,s,"V")}}validateKnots(e,t,i,r){let s=[],o=i+t+1;e.length!==o&&s.push(`${r} knot vector must have ${o} values (poles ${i} + degree ${t} + 1). Got ${e.length}.`);for(let a=1;a<e.length;a++)if(e[a]<e[a-1]){s.push(`${r} knot vector must be non-decreasing (index ${a}: ${e[a-1].toFixed(4)} > ${e[a].toFixed(4)}).`);break}if(e.length>=t+1){let a=0;for(;a<e.length&&e[a]===e[0];)a++;a<t+1&&s.push(`${r} start multiplicity must be \u2265 ${t+1} for a clamped curve. Got ${a}.`)}if(e.length>=t+1){let a=0,c=e[e.length-1];for(let l=e.length-1;l>=0&&e[l]===c;l--)a++;a<t+1&&s.push(`${r} end multiplicity must be \u2265 ${t+1} for a clamped curve. Got ${a}.`)}return{valid:s.length===0,errors:s}}parseKnotString(e){let t=e.trim().split(/[\s,]+/).filter(Boolean);if(t.length===0)return null;let i=t.map(Number);return i.some(isNaN)?null:i}knotsToString(e){return e.map(t=>t.toPrecision(7).replace(/\.?0+$/,"")).join(" ")}clonePatch(e){return ke(Ae({},e),{knotsU:[...e.knotsU],knotsV:[...e.knotsV],controlPoints:e.controlPoints.map(t=>t.map(i=>Ae({},i)))})}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ut({token:n,factory:n.\u0275fac,providedIn:"root"})};var ts=class n{selectedIdSubject=new lr(null);hoveredIdSubject=new lr(null);hoveredCpSubject=new lr(null);selectedId$=this.selectedIdSubject.asObservable();hoveredId$=this.hoveredIdSubject.asObservable();hoveredCp$=this.hoveredCpSubject.asObservable();get selectedId(){return this.selectedIdSubject.value}get hoveredId(){return this.hoveredIdSubject.value}get hoveredCp(){return this.hoveredCpSubject.value}select(e){this.selectedIdSubject.next(e)}hover(e){this.hoveredIdSubject.next(e)}hoverCp(e){this.hoveredCpSubject.next(e)}clear(){this.selectedIdSubject.next(null),this.hoveredIdSubject.next(null),this.hoveredCpSubject.next(null)}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ut({token:n,factory:n.\u0275fac,providedIn:"root"})};function uR(n,e){if(n&1){let t=br();pe(0,"tr",23)(1,"td",24),Ee(2),ge(),pe(3,"td",24),Ee(4),ge(),pe(5,"td")(6,"input",25),Zo(7,"number"),Pt("change",function(r){let s=Ct(t).$implicit,o=nt(2);return Tt(o.onCpFieldChange(s,"x",r))})("focus",function(){let r=Ct(t).$implicit,s=nt(2);return Tt(s.onCpFocus(r))})("blur",function(){let r=Ct(t).$implicit,s=nt(2);return Tt(s.onCpBlur(r))}),ge()(),pe(8,"td")(9,"input",25),Zo(10,"number"),Pt("change",function(r){let s=Ct(t).$implicit,o=nt(2);return Tt(o.onCpFieldChange(s,"y",r))})("focus",function(){let r=Ct(t).$implicit,s=nt(2);return Tt(s.onCpFocus(r))})("blur",function(){let r=Ct(t).$implicit,s=nt(2);return Tt(s.onCpBlur(r))}),ge()(),pe(11,"td")(12,"input",25),Zo(13,"number"),Pt("change",function(r){let s=Ct(t).$implicit,o=nt(2);return Tt(o.onCpFieldChange(s,"z",r))})("focus",function(){let r=Ct(t).$implicit,s=nt(2);return Tt(s.onCpFocus(r))})("blur",function(){let r=Ct(t).$implicit,s=nt(2);return Tt(s.onCpBlur(r))}),ge()(),pe(14,"td")(15,"input",26),Zo(16,"number"),Pt("change",function(r){let s=Ct(t).$implicit,o=nt(2);return Tt(o.onCpFieldChange(s,"w",r))})("focus",function(){let r=Ct(t).$implicit,s=nt(2);return Tt(s.onCpFocus(r))})("blur",function(){let r=Ct(t).$implicit,s=nt(2);return Tt(s.onCpBlur(r))}),ge()()()}if(n&2){let t=e.$implicit,i=nt(2);dn("row-start",t.ci===0)("highlighted",i.hoveredCpKey===t.ri+","+t.ci),bn("id","cp-row-"+t.ri+"-"+t.ci),Ce(2),fn(t.ri),Ce(2),fn(t.ci),Ce(2),bn("value",Ko(7,19,t.x,"1.3-6"))("id","cp_"+t.ri+"_"+t.ci+"_x"),xn("aria-label","CP "+t.ri+","+t.ci+" X"),Ce(3),bn("value",Ko(10,22,t.y,"1.3-6"))("id","cp_"+t.ri+"_"+t.ci+"_y"),xn("aria-label","CP "+t.ri+","+t.ci+" Y"),Ce(3),bn("value",Ko(13,25,t.z,"1.3-6"))("id","cp_"+t.ri+"_"+t.ci+"_z"),xn("aria-label","CP "+t.ri+","+t.ci+" Z"),Ce(3),bn("value",Ko(16,28,t.w,"1.4-6"))("id","cp_"+t.ri+"_"+t.ci+"_w"),xn("aria-label","CP "+t.ri+","+t.ci+" W")}}function dR(n,e){if(n&1&&(pe(0,"div",16)(1,"table",21)(2,"thead")(3,"tr")(4,"th"),Ee(5,"i"),ge(),pe(6,"th"),Ee(7,"j"),ge(),pe(8,"th"),Ee(9,"X"),ge(),pe(10,"th"),Ee(11,"Y"),ge(),pe(12,"th"),Ee(13,"Z"),ge(),pe(14,"th"),Ee(15,"W"),ge()()(),pe(16,"tbody"),_r(17,uR,17,31,"tr",22,nv().trackByCpIdx,!0),ge()()()),n&2){let t=nt();Ce(17),xr(t.flatCPs)}}function fR(n,e){if(n&1&&(pe(0,"li"),Ee(1),ge()),n&2){let t=e.$implicit;Ce(),fn(t)}}function hR(n,e){if(n&1&&(pe(0,"ul",31),_r(1,fR,2,1,"li",null,Rd),ge()),n&2){let t=nt(2);Ce(),xr(t.knotUResult.errors)}}function pR(n,e){if(n&1&&(pe(0,"li"),Ee(1),ge()),n&2){let t=e.$implicit;Ce(),fn(t)}}function mR(n,e){if(n&1&&(pe(0,"ul",31),_r(1,pR,2,1,"li",null,Rd),ge()),n&2){let t=nt(2);Ce(),xr(t.knotVResult.errors)}}function gR(n,e){if(n&1){let t=br();pe(0,"div",17)(1,"div",27)(2,"label",28),Ee(3," U Knots "),pe(4,"span",29),Ee(5),ge()(),pe(6,"textarea",30),wc("ngModelChange",function(r){Ct(t);let s=nt();return kd(s.localKnotsU,r)||(s.localKnotsU=r),Tt(r)}),Pt("input",function(){Ct(t);let r=nt();return Tt(r.onKnotUInput())}),ge(),Ad(),Un(7,hR,3,0,"ul",31),ge(),pe(8,"div",27)(9,"label",32),Ee(10," V Knots "),pe(11,"span",29),Ee(12),ge()(),pe(13,"textarea",33),wc("ngModelChange",function(r){Ct(t);let s=nt();return kd(s.localKnotsV,r)||(s.localKnotsV=r),Tt(r)}),Pt("input",function(){Ct(t);let r=nt();return Tt(r.onKnotVInput())}),ge(),Ad(),Un(14,mR,3,0,"ul",31),ge()()}if(n&2){let t=nt();Ce(5),Er("",t.patch.knotsU.length," values"),Ce(),dn("has-error",!t.knotUResult.valid),Mc("ngModel",t.localKnotsU),Id(),Ce(),Bn(t.knotUResult.valid?-1:7),Ce(5),Er("",t.patch.knotsV.length," values"),Ce(),dn("has-error",!t.knotVResult.valid),Mc("ngModel",t.localKnotsV),Id(),Ce(),Bn(t.knotVResult.valid?-1:14)}}var ef=class n{constructor(e,t,i,r){this.editorService=e;this.selectionService=t;this.ngZone=i;this.cdr=r}editorService;selectionService;ngZone;cdr;patch;patchEdited=new xt;activeTab="cp";hoveredCpKey=null;subscription=new Ft;flatCPs=[];localKnotsU="";localKnotsV="";knotUResult={valid:!0,errors:[]};knotVResult={valid:!0,errors:[]};ngOnInit(){this.subscription.add(this.selectionService.hoveredCp$.subscribe(e=>{this.ngZone.run(()=>{if(this.hoveredCpKey=e,this.cdr.detectChanges(),e){let[t,i]=e.split(",");setTimeout(()=>{let r=document.getElementById(`cp-row-${t}-${i}`);r&&r.scrollIntoView({behavior:"smooth",block:"center"})},0)}})}))}ngOnChanges(e){e.patch&&this.resetLocalState()}ngOnDestroy(){this.subscription.unsubscribe()}onCpFocus(e){this.selectionService.hoverCp(`${e.ri},${e.ci}`)}onCpBlur(e){this.selectionService.hoveredCp===`${e.ri},${e.ci}`&&this.selectionService.hoverCp(null)}resetLocalState(){this.flatCPs=this.patch.controlPoints.flatMap((e,t)=>e.map((i,r)=>({ri:t,ci:r,x:i.x,y:i.y,z:i.z,w:i.w}))),this.localKnotsU=this.editorService.knotsToString(this.patch.knotsU),this.localKnotsV=this.editorService.knotsToString(this.patch.knotsV),this.knotUResult={valid:!0,errors:[]},this.knotVResult={valid:!0,errors:[]}}onCpFieldChange(e,t,i){let r=parseFloat(i.target.value);isNaN(r)||(e[t]=r)}onKnotUInput(){this.knotUResult={valid:!0,errors:[]}}onKnotVInput(){this.knotVResult={valid:!0,errors:[]}}onApply(){let e=this.editorService.parseKnotString(this.localKnotsU),t=this.editorService.parseKnotString(this.localKnotsV);if(e||(this.knotUResult={valid:!1,errors:["Invalid format. Use space- or comma-separated numbers."]}),t||(this.knotVResult={valid:!1,errors:["Invalid format. Use space- or comma-separated numbers."]}),!e||!t){this.activeTab="knots";return}let i=this.editorService.validateKnotsForPatch(this.patch,e,t);if(this.knotUResult=i.u,this.knotVResult=i.v,!this.knotUResult.valid||!this.knotVResult.valid){this.activeTab="knots";return}let r=this.patch.controlPoints.length,s=this.patch.controlPoints[0]?.length??0,o=Array.from({length:r},(c,l)=>Array.from({length:s},(u,d)=>{let f=this.flatCPs.find(h=>h.ri===l&&h.ci===d);return f?{x:f.x,y:f.y,z:f.z,w:f.w}:Ae({},this.patch.controlPoints[l][d])})),a=ke(Ae({},this.patch),{controlPoints:o,knotsU:e,knotsV:t});this.patchEdited.emit(a)}onReset(){this.resetLocalState()}trackByCpIdx(e,t){return`${t.ri}_${t.ci}`}static \u0275fac=function(t){return new(t||n)(ct(Qd),ct(ts),ct(Rt),ct(Yi))};static \u0275cmp=yr({type:n,selectors:[["app-patch-editor"]],inputs:{patch:"patch"},outputs:{patchEdited:"patchEdited"},features:[gr],decls:43,vars:15,consts:[[1,"patch-editor"],[1,"editor-header"],["aria-hidden","true",1,"editor-icon"],[1,"editor-title-group"],[1,"editor-eyebrow"],[1,"editor-patch-name",3,"title"],[1,"editor-meta"],["title","U degree",1,"meta-chip"],[1,"chip-label"],[1,"chip-val"],["title","V degree",1,"meta-chip"],["title","Control grid",1,"meta-chip"],["title","Weight type",1,"meta-chip"],["role","tablist",1,"tab-bar"],["role","tab",1,"tab-btn",3,"click"],[1,"tab-content"],[1,"cp-table-wrapper"],[1,"knot-editor"],[1,"editor-footer"],["id","patch-editor-reset",1,"btn","btn-reset",3,"click"],["id","patch-editor-apply",1,"btn","btn-apply",3,"click"],[1,"cp-table"],[3,"row-start","highlighted","id"],[3,"id"],[1,"idx-cell"],["type","number","step","0.001",1,"cp-input",3,"change","focus","blur","value","id"],["type","number","step","0.0001","min","0.0001",1,"cp-input","w-input",3,"change","focus","blur","value","id"],[1,"knot-section"],["for","knotsU",1,"knot-label"],[1,"knot-count"],["id","knotsU","rows","3","spellcheck","false","placeholder","Space- or comma-separated knot values\u2026",1,"knot-textarea",3,"ngModelChange","input","ngModel"],[1,"error-list"],["for","knotsV",1,"knot-label"],["id","knotsV","rows","3","spellcheck","false","placeholder","Space- or comma-separated knot values\u2026",1,"knot-textarea",3,"ngModelChange","input","ngModel"]],template:function(t,i){t&1&&(pe(0,"div",0)(1,"div",1)(2,"span",2),Ee(3,"\u2B21"),ge(),pe(4,"div",3)(5,"span",4),Ee(6,"Surface Editor"),ge(),pe(7,"span",5),Ee(8),ge()()(),pe(9,"div",6)(10,"span",7)(11,"span",8),Ee(12,"U deg"),ge(),pe(13,"span",9),Ee(14),ge()(),pe(15,"span",10)(16,"span",8),Ee(17,"V deg"),ge(),pe(18,"span",9),Ee(19),ge()(),pe(20,"span",11)(21,"span",8),Ee(22,"Grid"),ge(),pe(23,"span",9),Ee(24),ge()(),pe(25,"span",12)(26,"span",8),Ee(27,"Poles"),ge(),pe(28,"span",9),Ee(29),ge()()(),pe(30,"div",13)(31,"button",14),Pt("click",function(){return i.activeTab="cp"}),Ee(32," Control Points "),ge(),pe(33,"button",14),Pt("click",function(){return i.activeTab="knots"}),Ee(34," Knot Vectors "),ge()(),pe(35,"div",15),Un(36,dR,19,0,"div",16),Un(37,gR,15,10,"div",17),ge(),pe(38,"div",18)(39,"button",19),Pt("click",function(){return i.onReset()}),Ee(40," Reset "),ge(),pe(41,"button",20),Pt("click",function(){return i.onApply()}),Ee(42," Apply "),ge()()()),t&2&&(Ce(7),bn("title",i.patch.name),Ce(),fn(i.patch.name),Ce(6),fn(i.patch.degreeU),Ce(5),fn(i.patch.degreeV),Ce(5),Fd("",i.patch.controlPoints.length,"\xD7",i.patch.controlPoints[0]?.length),Ce(5),Er(" ",i.patch.controlPoints.length*(i.patch.controlPoints[0]?.length??0)," "),Ce(2),dn("active",i.activeTab==="cp"),xn("aria-selected",i.activeTab==="cp"),Ce(2),dn("active",i.activeTab==="knots"),xn("aria-selected",i.activeTab==="knots"),Ce(3),Bn(i.activeTab==="cp"?36:-1),Ce(),Bn(i.activeTab==="knots"?37:-1))},dependencies:[jE,Kd,GE,Nv,hv],styles:['[_nghost-%COMP%]{display:flex;flex-direction:column;height:100%;min-height:0}.patch-editor[_ngcontent-%COMP%]{display:flex;flex-direction:column;height:100%;min-height:0;gap:.6rem;padding:.75rem .85rem;background:#ffffff08;border-top:1px solid rgba(255,255,255,.1);box-sizing:border-box}.editor-header[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.6rem;flex-shrink:0}.editor-icon[_ngcontent-%COMP%]{font-size:1.15rem;color:gold;filter:drop-shadow(0 0 4px rgba(255,215,0,.5));flex-shrink:0}.editor-title-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:.05rem;min-width:0}.editor-eyebrow[_ngcontent-%COMP%]{font-size:.65rem;letter-spacing:.12em;text-transform:uppercase;color:#ffffff73}.editor-patch-name[_ngcontent-%COMP%]{font-size:.88rem;font-weight:600;color:#e8f0ff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.editor-meta[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:.35rem;flex-shrink:0}.meta-chip[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:.3rem;padding:.2rem .55rem;border-radius:999px;background:#5b8def26;border:1px solid rgba(91,141,239,.3);font-size:.72rem}.chip-label[_ngcontent-%COMP%]{color:#ffffff80}.chip-val[_ngcontent-%COMP%]{color:#9ed0ff;font-weight:600}.tab-bar[_ngcontent-%COMP%]{display:flex;gap:.25rem;background:#ffffff0a;border:1px solid rgba(255,255,255,.08);border-radius:.7rem;padding:.2rem;flex-shrink:0}.tab-btn[_ngcontent-%COMP%]{flex:1;padding:.35rem .5rem;border:none;border-radius:.5rem;background:transparent;color:#ffffff80;font-size:.75rem;font-weight:500;cursor:pointer;transition:background .18s ease,color .18s ease}.tab-btn[_ngcontent-%COMP%]:hover{color:#ffffffd9;background:#ffffff0f}.tab-btn.active[_ngcontent-%COMP%]{background:linear-gradient(135deg,#5b8def66,#ff6bcb4d);color:#fff;font-weight:600;box-shadow:0 2px 8px #5b8def33}.tab-content[_ngcontent-%COMP%]{flex:1;min-height:0;overflow:hidden;display:flex;flex-direction:column}.cp-table-wrapper[_ngcontent-%COMP%]{flex:1;min-height:0;overflow:auto;border-radius:.55rem;border:1px solid rgba(255,255,255,.08)}.cp-table[_ngcontent-%COMP%]{width:100%;border-collapse:collapse;font-size:.72rem;min-width:340px}.cp-table[_ngcontent-%COMP%]   thead[_ngcontent-%COMP%]{position:sticky;top:0;z-index:1;background:#0b1028eb;-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px)}.cp-table[_ngcontent-%COMP%]   thead[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]{padding:.4rem .35rem;text-align:center;color:#ffffff80;font-weight:600;letter-spacing:.06em;border-bottom:1px solid rgba(255,255,255,.1);font-size:.68rem;text-transform:uppercase}.cp-table[_ngcontent-%COMP%]   tbody[_ngcontent-%COMP%]   tr[_ngcontent-%COMP%]{border-bottom:1px solid rgba(255,255,255,.04);transition:background .12s ease}.cp-table[_ngcontent-%COMP%]   tbody[_ngcontent-%COMP%]   tr[_ngcontent-%COMP%]:hover{background:#5b8def14}.cp-table[_ngcontent-%COMP%]   tbody[_ngcontent-%COMP%]   tr.highlighted[_ngcontent-%COMP%]{background:#ff8c0029}.cp-table[_ngcontent-%COMP%]   tbody[_ngcontent-%COMP%]   tr.highlighted[_ngcontent-%COMP%]   .cp-input[_ngcontent-%COMP%]{border-color:#ff8c0059;background:#ff8c0014}.cp-table[_ngcontent-%COMP%]   tbody[_ngcontent-%COMP%]   tr.row-start[_ngcontent-%COMP%]{border-top:1px solid rgba(255,215,0,.12)}.cp-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]{padding:.18rem .2rem;text-align:center}.idx-cell[_ngcontent-%COMP%]{color:#ffd700b3;font-weight:600;font-size:.68rem;padding:.18rem .4rem!important;min-width:1.8rem}.cp-input[_ngcontent-%COMP%]{width:100%;min-width:60px;padding:.22rem .28rem;background:#ffffff0f;border:1px solid rgba(255,255,255,.1);border-radius:.35rem;color:#e8f0ff;font-size:.7rem;font-family:Fira Code,Cascadia Code,monospace;text-align:right;transition:border-color .15s,background .15s;box-sizing:border-box}.cp-input[_ngcontent-%COMP%]:focus{outline:none;border-color:#5b8def99;background:#5b8def1a}.cp-input[_ngcontent-%COMP%]:hover{border-color:#ffffff38}.w-input[_ngcontent-%COMP%]{color:gold}.cp-input[_ngcontent-%COMP%]::-webkit-outer-spin-button, .cp-input[_ngcontent-%COMP%]::-webkit-inner-spin-button{-webkit-appearance:none}.cp-input[type=number][_ngcontent-%COMP%]{-moz-appearance:textfield}.knot-editor[_ngcontent-%COMP%]{flex:1;min-height:0;display:flex;flex-direction:column;gap:.7rem;overflow-y:auto;padding-right:.15rem}.knot-section[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:.3rem}.knot-label[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;font-size:.73rem;font-weight:600;color:#ffffffb3}.knot-count[_ngcontent-%COMP%]{font-weight:400;color:#ffffff61;font-size:.68rem}.knot-textarea[_ngcontent-%COMP%]{width:100%;padding:.45rem .55rem;background:#ffffff0d;border:1px solid rgba(255,255,255,.1);border-radius:.5rem;color:#c8e0ff;font-size:.7rem;font-family:Fira Code,Cascadia Code,monospace;resize:vertical;line-height:1.6;transition:border-color .15s,background .15s;box-sizing:border-box}.knot-textarea[_ngcontent-%COMP%]:focus{outline:none;border-color:#5b8def8c;background:#5b8def14}.knot-textarea.has-error[_ngcontent-%COMP%]{border-color:#ff646480;background:#ff3c3c0f}.error-list[_ngcontent-%COMP%]{margin:0;padding:.4rem .55rem;list-style:none;background:#ff3c3c1a;border:1px solid rgba(255,80,80,.25);border-radius:.4rem;display:flex;flex-direction:column;gap:.2rem}.error-list[_ngcontent-%COMP%]   li[_ngcontent-%COMP%]{font-size:.68rem;color:#ff8da1;line-height:1.4}.error-list[_ngcontent-%COMP%]   li[_ngcontent-%COMP%]:before{content:"\\26a0  "}.editor-footer[_ngcontent-%COMP%]{display:flex;gap:.5rem;flex-shrink:0}.btn[_ngcontent-%COMP%]{flex:1;padding:.52rem .8rem;border:none;border-radius:.6rem;font-size:.78rem;font-weight:600;cursor:pointer;transition:transform .15s ease,box-shadow .15s ease,opacity .15s ease}.btn[_ngcontent-%COMP%]:active{transform:scale(.97)}.btn-reset[_ngcontent-%COMP%]{background:#ffffff14;border:1px solid rgba(255,255,255,.14);color:#ffffffb3}.btn-reset[_ngcontent-%COMP%]:hover{background:#ffffff21;color:#fff}.btn-apply[_ngcontent-%COMP%]{background:linear-gradient(135deg,#3d79e8,#a855b5);color:#fff;box-shadow:0 4px 16px #5b8def4d}.btn-apply[_ngcontent-%COMP%]:hover{box-shadow:0 6px 24px #5b8def73;transform:translateY(-1px)}']})};var vR=(n,e)=>e.id;function yR(n,e){n&1&&(pe(0,"p",12),Ee(1,"Parsing STEP geometry\u2026"),ge())}function _R(n,e){if(n&1&&(pe(0,"p",13),Ee(1),ge()),n&2){let t=nt();Ce(),fn(t.error)}}function xR(n,e){if(n&1&&(pe(0,"p",14),Ee(1),ge()),n&2){let t=nt();Ce(),fn(t.fileName)}}function bR(n,e){if(n&1){let t=br();pe(0,"li",18),Pt("mouseenter",function(){let r=Ct(t).$implicit,s=nt();return Tt(s.onPatchEnter(r.id))})("mouseleave",function(){Ct(t);let r=nt();return Tt(r.onPatchLeave())})("click",function(){let r=Ct(t).$implicit,s=nt();return Tt(s.onPatchClick(r.id))}),Ec(1,"span",19),pe(2,"div",20)(3,"span",21),Ee(4),ge(),pe(5,"span",22),Ee(6),ge()()()}if(n&2){let t=e.$implicit,i=nt();dn("selected",t.id===i.selectedId)("hovered",t.id===i.hoveredId),Ce(4),fn(t.name),Ce(2),Ld(" U",t.degreeU," \xB7 V",t.degreeV," \xB7 ",t.controlPoints.length,"\xD7",t.controlPoints[0].length," poles ")}}function ER(n,e){n&1&&(pe(0,"li",23),Ee(1,"Upload a STEP file to extract NURBS surfaces."),ge())}function SR(n,e){if(n&1&&Un(0,ER,2,0,"li",23),n&2){let t=nt();Bn(!t.loading&&!t.error?0:-1)}}function MR(n,e){if(n&1){let t=br();pe(0,"div",24)(1,"div",25),Pt("mousedown",function(r){Ct(t);let s=nt();return Tt(s.onResizeStart(r))}),Ec(2,"div",26),ge(),pe(3,"app-patch-editor",27),Pt("patchEdited",function(r){Ct(t);let s=nt();return Tt(s.onPatchEdited(r))}),ge()()}if(n&2){let t=nt();Od("height",t.editorHeight,"px"),Ce(),dn("active",t.isResizing),xn("aria-valuenow",t.editorHeight),Ce(2),bn("patch",t.selectedPatch)}}var tf=class n{patches=[];selectedId=null;hoveredId=null;loading=!1;error=null;fileName=null;selectedPatch=null;patchHover=new xt;patchSelect=new xt;fileSelected=new xt;patchEdited=new xt;patchesLoaded=new xt;exportStep=new xt;onFileChange(e){let t=e.target,i=t.files?.[0];i&&(this.fileSelected.emit(i),t.value="")}onPatchEnter(e){this.patchHover.emit(e)}onPatchLeave(){this.patchHover.emit(null)}onPatchClick(e){this.patchSelect.emit(e)}onPatchEdited(e){this.patchEdited.emit(e)}savePatchesToJson(){if(!this.patches.length)return;let e={version:"1.0",exportedAt:new Date().toISOString(),patchCount:this.patches.length,patches:this.patches},t=new Blob([JSON.stringify(e,null,2)],{type:"application/json;charset=utf-8"}),i=URL.createObjectURL(t),r=document.createElement("a");r.href=i,r.download="nurbs-model.json",document.body.appendChild(r),r.click(),r.remove(),setTimeout(()=>URL.revokeObjectURL(i),0)}loadPatchesFromJson(e){let t=e.target;if(!t.files?.length)return;let i=t.files[0],r=new FileReader;r.onload=s=>{try{let o=s.target?.result,a=JSON.parse(o);a&&a.patches&&Array.isArray(a.patches)?this.patchesLoaded.emit(a.patches):console.error("Invalid JSON format: missing patches array")}catch(o){console.error("Failed to parse JSON file",o)}finally{t.value=""}},r.readAsText(i)}editorHeight=360;isResizing=!1;startY=0;startHeight=0;onMouseMoveRef=this.onMouseMove.bind(this);onMouseUpRef=this.onMouseUp.bind(this);onResizeStart(e){e.preventDefault(),this.isResizing=!0,this.startY=e.clientY,this.startHeight=this.editorHeight,window.addEventListener("mousemove",this.onMouseMoveRef),window.addEventListener("mouseup",this.onMouseUpRef)}onMouseMove(e){if(!this.isResizing)return;let t=this.startY-e.clientY,i=this.startHeight+t,r=180,s=window.innerHeight*.75;this.editorHeight=Math.max(r,Math.min(i,s))}onMouseUp(){this.isResizing=!1,window.removeEventListener("mousemove",this.onMouseMoveRef),window.removeEventListener("mouseup",this.onMouseUpRef)}ngOnDestroy(){window.removeEventListener("mousemove",this.onMouseMoveRef),window.removeEventListener("mouseup",this.onMouseUpRef)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=yr({type:n,selectors:[["app-feature-tree"]],inputs:{patches:"patches",selectedId:"selectedId",hoveredId:"hoveredId",loading:"loading",error:"error",fileName:"fileName",selectedPatch:"selectedPatch"},outputs:{patchHover:"patchHover",patchSelect:"patchSelect",fileSelected:"fileSelected",patchEdited:"patchEdited",patchesLoaded:"patchesLoaded",exportStep:"exportStep"},decls:31,vars:10,consts:[["jsonFileInput",""],[1,"feature-tree"],[1,"tree-header"],[1,"eyebrow"],[1,"patch-count"],[1,"file-actions"],[1,"upload-button","upload-button--import"],["type","file","accept",".step,.stp,.STEP,.STP",3,"change"],[1,"upload-button","upload-button--load"],["type","file","accept",".json",3,"change"],["type","button","aria-label","Export NURBS patches to STEP file",1,"upload-button","upload-button--export",3,"click","disabled"],["type","button","aria-label","Save NURBS patches as JSON",1,"upload-button","upload-button--save",3,"click","disabled"],[1,"status","loading"],[1,"status","error"],[1,"file-name"],["role","tree",1,"patch-list"],["role","treeitem",1,"patch-item",3,"selected","hovered"],[1,"editor-container",3,"height"],["role","treeitem",1,"patch-item",3,"mouseenter","mouseleave","click"],["aria-hidden","true",1,"patch-icon"],[1,"patch-copy"],[1,"patch-name"],[1,"patch-meta"],[1,"empty-state"],[1,"editor-container"],["role","slider","aria-label","Resize editor panel","aria-valuemin","180","aria-valuemax","600","tabindex","0",1,"resize-handle",3,"mousedown"],[1,"resize-knob"],[3,"patchEdited","patch"]],template:function(t,i){t&1&&(pe(0,"section",1)(1,"header",2)(2,"div")(3,"p",3),Ee(4,"Feature Tree"),ge(),pe(5,"h2"),Ee(6,"NURBS Patches"),ge()(),pe(7,"span",4),Ee(8),ge()(),pe(9,"div",5)(10,"label",6)(11,"input",7),Pt("change",function(s){return i.onFileChange(s)}),ge(),pe(12,"span"),Ee(13,"Import STEP"),ge()(),pe(14,"label",8)(15,"input",9,0),Pt("change",function(s){return i.loadPatchesFromJson(s)}),ge(),pe(17,"span"),Ee(18,"Load JSON"),ge()(),pe(19,"button",10),Pt("click",function(){return i.exportStep.emit()}),Ee(20,"Export STEP"),ge(),pe(21,"button",11),Pt("click",function(){return i.savePatchesToJson()}),Ee(22,"Save JSON"),ge()(),Un(23,yR,2,0,"p",12),Un(24,_R,2,1,"p",13),Un(25,xR,2,1,"p",14),pe(26,"ul",15),_r(27,bR,7,9,"li",16,vR,!1,SR,1,1),ge(),Un(30,MR,4,6,"div",17),ge()),t&2&&(Ce(8),fn(i.patches.length),Ce(11),bn("disabled",i.patches.length===0),Ce(2),bn("disabled",i.patches.length===0),Ce(2),Bn(i.loading?23:-1),Ce(),Bn(i.error?24:-1),Ce(),Bn(i.fileName&&!i.loading?25:-1),Ce(),dn("with-editor",i.selectedPatch!==null),Ce(),xr(i.patches),Ce(3),Bn(i.selectedPatch?30:-1))},dependencies:[ef],styles:["[_nghost-%COMP%]{display:flex;flex-direction:column;height:100vh;max-height:100vh;overflow:hidden;box-sizing:border-box}.feature-tree[_ngcontent-%COMP%]{display:flex;flex-direction:column;height:100%;min-height:0;overflow:hidden}.patch-list[_ngcontent-%COMP%]{flex:1 1 0%;min-height:0;overflow-y:auto;margin:0;padding:0;list-style:none}.editor-container[_ngcontent-%COMP%]{position:relative;flex-shrink:0;display:flex;flex-direction:column;overflow:hidden;min-height:150px;max-height:80%;background:#ffffff03;border-top:1px solid rgba(255,255,255,.08)}.tree-header[_ngcontent-%COMP%]{display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;flex-shrink:0}.eyebrow[_ngcontent-%COMP%]{margin:0 0 .25rem;font-size:.75rem;letter-spacing:.12em;text-transform:uppercase;color:#ffffff8c}.tree-header[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{margin:0;font-size:1.35rem;font-weight:600}.patch-count[_ngcontent-%COMP%]{display:inline-flex;align-items:center;justify-content:center;min-width:2rem;height:2rem;padding:0 .5rem;border-radius:999px;background:#ffffff14;color:#9ed0ff;font-weight:600}.file-actions[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr 1fr;gap:.5rem;flex-shrink:0}.upload-button[_ngcontent-%COMP%]{display:inline-flex;align-items:center;justify-content:center;padding:.65rem .75rem;border-radius:.9rem;border:1px solid rgba(255,255,255,.18);color:#fff;font-size:.8rem;font-weight:600;cursor:pointer;transition:transform .2s ease,box-shadow .2s ease,opacity .2s ease;flex-shrink:0;text-align:center;background:linear-gradient(135deg,#5b8def59,#ff6bcb47)}.upload-button--import[_ngcontent-%COMP%]{background:linear-gradient(135deg,#5b8def59,#ff6bcb47)}.upload-button--import[_ngcontent-%COMP%]:hover{box-shadow:0 10px 30px #5b8def40}.upload-button--save[_ngcontent-%COMP%]{background:linear-gradient(135deg,#34d39959,#10b98147)}.upload-button--save[_ngcontent-%COMP%]:hover:not(:disabled){box-shadow:0 10px 30px #34d39940}.upload-button--load[_ngcontent-%COMP%]{background:linear-gradient(135deg,#fbbf2459,#f59e0b47)}.upload-button--load[_ngcontent-%COMP%]:hover{box-shadow:0 10px 30px #fbbf2440}.upload-button--export[_ngcontent-%COMP%]{background:linear-gradient(135deg,#a78bfa59,#8b5cf647)}.upload-button--export[_ngcontent-%COMP%]:hover:not(:disabled){box-shadow:0 10px 30px #a78bfa40}.upload-button[_ngcontent-%COMP%]:hover:not(:disabled){transform:translateY(-1px)}.upload-button[_ngcontent-%COMP%]:disabled{opacity:.45;cursor:not-allowed}.upload-button[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{display:none}.status[_ngcontent-%COMP%]{margin:0;font-size:.9rem;flex-shrink:0}.status.loading[_ngcontent-%COMP%]{color:#9ed0ff}.status.error[_ngcontent-%COMP%]{color:#ff8da1}.file-name[_ngcontent-%COMP%]{margin:0;font-size:.85rem;color:#ffffffa6;word-break:break-all;flex-shrink:0}.patch-item[_ngcontent-%COMP%], .empty-state[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.75rem;padding:.85rem .95rem;border-radius:.9rem;border:1px solid rgba(255,255,255,.08);background:#ffffff0a;transition:background .2s ease,border-color .2s ease,transform .2s ease}.patch-item[_ngcontent-%COMP%]{cursor:pointer;flex-shrink:0}.patch-item[_ngcontent-%COMP%]:hover, .patch-item.hovered[_ngcontent-%COMP%]{background:#66e0ff1f;border-color:#66e0ff59}.patch-item.selected[_ngcontent-%COMP%]{background:#ff6bcb29;border-color:#ff6bcb73;transform:translate(4px)}.patch-icon[_ngcontent-%COMP%]{width:.75rem;height:.75rem;border-radius:.2rem;background:linear-gradient(135deg,#5b8def,#66e0ff);flex-shrink:0}.patch-copy[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:.2rem;min-width:0}.patch-name[_ngcontent-%COMP%]{font-weight:600}.patch-meta[_ngcontent-%COMP%]{font-size:.78rem;color:#ffffff8c}.empty-state[_ngcontent-%COMP%]{color:#ffffff8c;font-size:.92rem}@keyframes _ngcontent-%COMP%_editor-slide-in{0%{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}.resize-handle[_ngcontent-%COMP%]{position:absolute;top:-4px;left:0;right:0;height:8px;cursor:ns-resize;z-index:50;display:flex;align-items:center;justify-content:center;background:transparent;transition:background .15s ease}.resize-handle[_ngcontent-%COMP%]:hover, .resize-handle.active[_ngcontent-%COMP%]{background:#5b8def1f}.resize-knob[_ngcontent-%COMP%]{width:40px;height:4px;background:#fff3;border-radius:2px;transition:background .15s ease,width .15s ease,box-shadow .15s ease}.resize-handle[_ngcontent-%COMP%]:hover   .resize-knob[_ngcontent-%COMP%], .resize-handle.active[_ngcontent-%COMP%]   .resize-knob[_ngcontent-%COMP%]{background:#3d79e8;width:56px;box-shadow:0 0 8px #3d79e8d9}"]})};var gs={LEFT:0,MIDDLE:1,RIGHT:2,ROTATE:0,DOLLY:1,PAN:2},vs={ROTATE:0,PAN:1,DOLLY_PAN:2,DOLLY_ROTATE:3},SS=0,vy=1,MS=2;var Sl=1,wS=2,Ua=3,Rr=0,Pn=1,fi=2,tr=0,so=1,yy=2,_y=3,xy=4,CS=5;var cs=100,TS=101,DS=102,AS=103,IS=104,RS=200,NS=201,PS=202,OS=203,Rf=204,Nf=205,FS=206,LS=207,kS=208,US=209,BS=210,VS=211,HS=212,zS=213,GS=214,Pf=0,Of=1,Ff=2,oo=3,Lf=4,kf=5,Uf=6,Bf=7,by=0,jS=1,WS=2,Ai=0,Ey=1,Sy=2,My=3,wy=4,Cy=5,Ty=6,Dy=7;var oy=300,ys=301,ao=302,ph=303,mh=304,Ml=306,Vf=1e3,Ji=1001,Hf=1002,an=1003,$S=1004;var wl=1005;var hn=1006,gh=1007;var _s=1008;var zn=1009,Ay=1010,Iy=1011,Ba=1012,vh=1013,Ii=1014,Ri=1015,nr=1016,yh=1017,_h=1018,Va=1020,Ry=35902,Ny=35899,Py=1021,Oy=1022,hi=1023,Qi=1026,xs=1027,Fy=1028,xh=1029,bs=1030,bh=1031;var Eh=1033,Cl=33776,Tl=33777,Dl=33778,Al=33779,Sh=35840,Mh=35841,wh=35842,Ch=35843,Th=36196,Dh=37492,Ah=37496,Ih=37488,Rh=37489,Il=37490,Nh=37491,Ph=37808,Oh=37809,Fh=37810,Lh=37811,kh=37812,Uh=37813,Bh=37814,Vh=37815,Hh=37816,zh=37817,Gh=37818,jh=37819,Wh=37820,$h=37821,qh=36492,Xh=36494,Yh=36495,Zh=36283,Kh=36284,Rl=36285,Jh=36286;var Zc=2300,zf=2301,If=2302,ay=2303,cy=2400,ly=2401,uy=2402;var qS=3200;var Qh=0,XS=1,Pr="",Cn="srgb",Kc="srgb-linear",Jc="linear",pt="srgb";var ro=7680;var dy=519,YS=512,ZS=513,KS=514,ep=515,JS=516,QS=517,tp=518,eM=519,Gf=35044;var Ly="300 es",Ti=2e3,Ea=2001;function wR(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function CR(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function Qc(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function tM(){let n=Qc("canvas");return n.style.display="block",n}var WE={},Sa=null;function el(...n){let e="THREE."+n.shift();Sa?Sa("log",e,...n):console.log(e,...n)}function nM(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Re(...n){n=nM(n);let e="THREE."+n.shift();if(Sa)Sa("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Pe(...n){n=nM(n);let e="THREE."+n.shift();if(Sa)Sa("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function jf(...n){let e=n.join(" ");e in WE||(WE[e]=!0,Re(...n))}function iM(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var rM={[Pf]:Of,[Ff]:Uf,[Lf]:Bf,[oo]:kf,[Of]:Pf,[Uf]:Ff,[Bf]:Lf,[kf]:oo},Di=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},Mn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],$E=1234567,Xc=Math.PI/180,Ma=180/Math.PI;function Ar(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(Mn[n&255]+Mn[n>>8&255]+Mn[n>>16&255]+Mn[n>>24&255]+"-"+Mn[e&255]+Mn[e>>8&255]+"-"+Mn[e>>16&15|64]+Mn[e>>24&255]+"-"+Mn[t&63|128]+Mn[t>>8&255]+"-"+Mn[t>>16&255]+Mn[t>>24&255]+Mn[i&255]+Mn[i>>8&255]+Mn[i>>16&255]+Mn[i>>24&255]).toLowerCase()}function Be(n,e,t){return Math.max(e,Math.min(t,n))}function ky(n,e){return(n%e+e)%e}function TR(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function DR(n,e,t){return n!==e?(t-n)/(e-n):0}function Yc(n,e,t){return(1-t)*n+t*e}function AR(n,e,t,i){return Yc(n,e,1-Math.exp(-t*i))}function IR(n,e=1){return e-Math.abs(ky(n,e*2)-e)}function RR(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function NR(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function PR(n,e){return n+Math.floor(Math.random()*(e-n+1))}function OR(n,e){return n+Math.random()*(e-n)}function FR(n){return n*(.5-Math.random())}function LR(n){n!==void 0&&($E=n);let e=$E+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function kR(n){return n*Xc}function UR(n){return n*Ma}function BR(n){return(n&n-1)===0&&n!==0}function VR(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function HR(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function zR(n,e,t,i,r){let s=Math.cos,o=Math.sin,a=s(t/2),c=o(t/2),l=s((e+i)/2),u=o((e+i)/2),d=s((e-i)/2),f=o((e-i)/2),h=s((i-e)/2),g=o((i-e)/2);switch(r){case"XYX":n.set(a*u,c*d,c*f,a*l);break;case"YZY":n.set(c*f,a*u,c*d,a*l);break;case"ZXZ":n.set(c*d,c*f,a*u,a*l);break;case"XZX":n.set(a*u,c*g,c*h,a*l);break;case"YXY":n.set(c*h,a*u,c*g,a*l);break;case"ZYZ":n.set(c*g,c*h,a*u,a*l);break;default:Re("MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function Ci(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function gt(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var Es={DEG2RAD:Xc,RAD2DEG:Ma,generateUUID:Ar,clamp:Be,euclideanModulo:ky,mapLinear:TR,inverseLerp:DR,lerp:Yc,damp:AR,pingpong:IR,smoothstep:RR,smootherstep:NR,randInt:PR,randFloat:OR,randFloatSpread:FR,seededRandom:LR,degToRad:kR,radToDeg:UR,isPowerOfTwo:BR,ceilPowerOfTwo:VR,floorPowerOfTwo:HR,setQuaternionFromProperEuler:zR,normalize:gt,denormalize:Ci},Te=class n{static{n.prototype.isVector2=!0}constructor(e=0,t=0){this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Be(this.x,e.x,t.x),this.y=Be(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=Be(this.x,e,t),this.y=Be(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Be(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Be(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},pn=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],f=s[o+0],h=s[o+1],g=s[o+2],y=s[o+3];if(d!==y||c!==f||l!==h||u!==g){let p=c*f+l*h+u*g+d*y;p<0&&(f=-f,h=-h,g=-g,y=-y,p=-p);let m=1-a;if(p<.9995){let b=Math.acos(p),M=Math.sin(b);m=Math.sin(m*b)/M,a=Math.sin(a*b)/M,c=c*m+f*a,l=l*m+h*a,u=u*m+g*a,d=d*m+y*a}else{c=c*m+f*a,l=l*m+h*a,u=u*m+g*a,d=d*m+y*a;let b=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=b,l*=b,u*=b,d*=b}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=s[o],f=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*d+c*h-l*f,e[t+1]=c*g+u*f+l*d-a*h,e[t+2]=l*g+u*h+a*f-c*d,e[t+3]=u*g-a*d-c*f-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(s/2),f=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"YXZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"ZXY":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"ZYX":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"YZX":this._x=f*u*d+l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d-f*h*g;break;case"XZY":this._x=f*u*d-l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d+f*h*g;break;default:Re("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(Be(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},T=class n{static{n.prototype.isVector3=!0}constructor(e=0,t=0,i=0){this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(qE.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(qE.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+c*l+o*d-a*u,this.y=i+c*u+a*l-s*d,this.z=r+c*d+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Be(this.x,e.x,t.x),this.y=Be(this.y,e.y,t.y),this.z=Be(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=Be(this.x,e,t),this.y=Be(this.y,e,t),this.z=Be(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Be(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return Ov.copy(this).projectOnVector(e),this.sub(Ov)}reflect(e){return this.sub(Ov.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Be(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},Ov=new T,qE=new pn,je=class n{static{n.prototype.isMatrix3=!0}constructor(e,t,i,r,s,o,a,c,l){this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],f=i[2],h=i[5],g=i[8],y=r[0],p=r[3],m=r[6],b=r[1],M=r[4],S=r[7],A=r[2],w=r[5],I=r[8];return s[0]=o*y+a*b+c*A,s[3]=o*p+a*M+c*w,s[6]=o*m+a*S+c*I,s[1]=l*y+u*b+d*A,s[4]=l*p+u*M+d*w,s[7]=l*m+u*S+d*I,s[2]=f*y+h*b+g*A,s[5]=f*p+h*M+g*w,s[8]=f*m+h*S+g*I,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*o-a*l,f=a*c-u*s,h=l*s-o*c,g=t*d+i*f+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let y=1/g;return e[0]=d*y,e[1]=(r*l-u*i)*y,e[2]=(a*i-r*o)*y,e[3]=f*y,e[4]=(u*t-r*c)*y,e[5]=(r*s-a*t)*y,e[6]=h*y,e[7]=(i*c-l*t)*y,e[8]=(o*t-i*s)*y,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(Fv.makeScale(e,t)),this}rotate(e){return this.premultiply(Fv.makeRotation(-e)),this}translate(e,t){return this.premultiply(Fv.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Fv=new je,XE=new je().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),YE=new je().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function GR(){let n={enabled:!0,workingColorSpace:Kc,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===pt&&(r.r=Ir(r.r),r.g=Ir(r.g),r.b=Ir(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===pt&&(r.r=ba(r.r),r.g=ba(r.g),r.b=ba(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===Pr?Jc:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return jf("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return jf("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Kc]:{primaries:e,whitePoint:i,transfer:Jc,toXYZ:XE,fromXYZ:YE,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Cn},outputColorSpaceConfig:{drawingBufferColorSpace:Cn}},[Cn]:{primaries:e,whitePoint:i,transfer:pt,toXYZ:XE,fromXYZ:YE,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Cn}}}),n}var at=GR();function Ir(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function ba(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var ia,Wf=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{ia===void 0&&(ia=Qc("canvas")),ia.width=e.width,ia.height=e.height;let r=ia.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=ia}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Qc("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Ir(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Ir(t[i]/255)*255):t[i]=Ir(t[i]);return{data:t,width:e.width,height:e.height}}else return Re("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},jR=0,wa=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:jR++}),this.uuid=Ar(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayWidth,t.displayHeight,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Lv(r[o].image)):s.push(Lv(r[o]))}else s=Lv(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Lv(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Wf.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Re("Texture: Unable to serialize Texture."),{})}var WR=0,kv=new T,ir=(()=>{class n extends Di{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Ji,s=Ji,o=hn,a=_s,c=hi,l=zn,u=n.DEFAULT_ANISOTROPY,d=Pr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:WR++}),this.uuid=Ar(),this.name="",this.source=new wa(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new Te(0,0),this.repeat=new Te(1,1),this.center=new Te(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new je,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0,this.normalized=!1}get width(){return this.source.getSize(kv).x}get height(){return this.source.getSize(kv).y}get depth(){return this.source.getSize(kv).z}get image(){return this.source.data}set image(t){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.normalized=t.normalized,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Re(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){Re(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,normalized:this.normalized,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==oy)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Vf:t.x=t.x-Math.floor(t.x);break;case Ji:t.x=t.x<0?0:1;break;case Hf:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Vf:t.y=t.y-Math.floor(t.y);break;case Ji:t.y=t.y<0?0:1;break;case Hf:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=oy,n.DEFAULT_ANISOTROPY=1,n})(),it=class n{static{n.prototype.isVector4=!0}constructor(e=0,t=0,i=0,r=1){this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],d=c[8],f=c[1],h=c[5],g=c[9],y=c[2],p=c[6],m=c[10];if(Math.abs(u-f)<.01&&Math.abs(d-y)<.01&&Math.abs(g-p)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+y)<.1&&Math.abs(g+p)<.1&&Math.abs(l+h+m-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let M=(l+1)/2,S=(h+1)/2,A=(m+1)/2,w=(u+f)/4,I=(d+y)/4,_=(g+p)/4;return M>S&&M>A?M<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(M),r=w/i,s=I/i):S>A?S<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(S),i=w/r,s=_/r):A<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(A),i=I/s,r=_/s),this.set(i,r,s,t),this}let b=Math.sqrt((p-g)*(p-g)+(d-y)*(d-y)+(f-u)*(f-u));return Math.abs(b)<.001&&(b=1),this.x=(p-g)/b,this.y=(d-y)/b,this.z=(f-u)/b,this.w=Math.acos((l+h+m-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Be(this.x,e.x,t.x),this.y=Be(this.y,e.y,t.y),this.z=Be(this.z,e.z,t.z),this.w=Be(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=Be(this.x,e,t),this.y=Be(this.y,e,t),this.z=Be(this.z,e,t),this.w=Be(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Be(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},$f=class extends Di{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:hn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new it(0,0,e,t),this.scissorTest=!1,this.viewport=new it(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new ir(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:hn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new wa(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this.multiview=e.multiview,this}dispose(){this.dispatchEvent({type:"dispose"})}},Kn=class extends $f{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},tl=class extends ir{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=an,this.minFilter=an,this.wrapR=Ji,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var qf=class extends ir{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=an,this.minFilter=an,this.wrapR=Ji,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Dt=class n{static{n.prototype.isMatrix4=!0}constructor(e,t,i,r,s,o,a,c,l,u,d,f,h,g,y,p){this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,y,p)}set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,y,p){let m=this.elements;return m[0]=e,m[4]=t,m[8]=i,m[12]=r,m[1]=s,m[5]=o,m[9]=a,m[13]=c,m[2]=l,m[6]=u,m[10]=d,m[14]=f,m[3]=h,m[7]=g,m[11]=y,m[15]=p,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/ra.setFromMatrixColumn(e,0).length(),s=1/ra.setFromMatrixColumn(e,1).length(),o=1/ra.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let f=o*u,h=o*d,g=a*u,y=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=h+g*l,t[5]=f-y*l,t[9]=-a*c,t[2]=y-f*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let f=c*u,h=c*d,g=l*u,y=l*d;t[0]=f+y*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=y+f*a,t[10]=o*c}else if(e.order==="ZXY"){let f=c*u,h=c*d,g=l*u,y=l*d;t[0]=f-y*a,t[4]=-o*d,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=y-f*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let f=o*u,h=o*d,g=a*u,y=a*d;t[0]=c*u,t[4]=g*l-h,t[8]=f*l+y,t[1]=c*d,t[5]=y*l+f,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let f=o*c,h=o*l,g=a*c,y=a*l;t[0]=c*u,t[4]=y-f*d,t[8]=g*d+h,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*d+g,t[10]=f-y*d}else if(e.order==="XZY"){let f=o*c,h=o*l,g=a*c,y=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=f*d+y,t[5]=o*u,t[9]=h*d-g,t[2]=g*d-h,t[6]=a*u,t[10]=y*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose($R,e,qR)}lookAt(e,t,i){let r=this.elements;return Yn.subVectors(e,t),Yn.lengthSq()===0&&(Yn.z=1),Yn.normalize(),ns.crossVectors(i,Yn),ns.lengthSq()===0&&(Math.abs(i.z)===1?Yn.x+=1e-4:Yn.z+=1e-4,Yn.normalize(),ns.crossVectors(i,Yn)),ns.normalize(),nf.crossVectors(Yn,ns),r[0]=ns.x,r[4]=nf.x,r[8]=Yn.x,r[1]=ns.y,r[5]=nf.y,r[9]=Yn.y,r[2]=ns.z,r[6]=nf.z,r[10]=Yn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],f=i[9],h=i[13],g=i[2],y=i[6],p=i[10],m=i[14],b=i[3],M=i[7],S=i[11],A=i[15],w=r[0],I=r[4],_=r[8],C=r[12],O=r[1],D=r[5],L=r[9],G=r[13],W=r[2],P=r[6],H=r[10],U=r[14],J=r[3],Q=r[7],ce=r[11],xe=r[15];return s[0]=o*w+a*O+c*W+l*J,s[4]=o*I+a*D+c*P+l*Q,s[8]=o*_+a*L+c*H+l*ce,s[12]=o*C+a*G+c*U+l*xe,s[1]=u*w+d*O+f*W+h*J,s[5]=u*I+d*D+f*P+h*Q,s[9]=u*_+d*L+f*H+h*ce,s[13]=u*C+d*G+f*U+h*xe,s[2]=g*w+y*O+p*W+m*J,s[6]=g*I+y*D+p*P+m*Q,s[10]=g*_+y*L+p*H+m*ce,s[14]=g*C+y*G+p*U+m*xe,s[3]=b*w+M*O+S*W+A*J,s[7]=b*I+M*D+S*P+A*Q,s[11]=b*_+M*L+S*H+A*ce,s[15]=b*C+M*G+S*U+A*xe,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],f=e[10],h=e[14],g=e[3],y=e[7],p=e[11],m=e[15],b=c*h-l*f,M=a*h-l*d,S=a*f-c*d,A=o*h-l*u,w=o*f-c*u,I=o*d-a*u;return t*(y*b-p*M+m*S)-i*(g*b-p*A+m*w)+r*(g*M-y*A+m*I)-s*(g*S-y*w+p*I)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],f=e[10],h=e[11],g=e[12],y=e[13],p=e[14],m=e[15],b=t*a-i*o,M=t*c-r*o,S=t*l-s*o,A=i*c-r*a,w=i*l-s*a,I=r*l-s*c,_=u*y-d*g,C=u*p-f*g,O=u*m-h*g,D=d*p-f*y,L=d*m-h*y,G=f*m-h*p,W=b*G-M*L+S*D+A*O-w*C+I*_;if(W===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let P=1/W;return e[0]=(a*G-c*L+l*D)*P,e[1]=(r*L-i*G-s*D)*P,e[2]=(y*I-p*w+m*A)*P,e[3]=(f*w-d*I-h*A)*P,e[4]=(c*O-o*G-l*C)*P,e[5]=(t*G-r*O+s*C)*P,e[6]=(p*S-g*I-m*M)*P,e[7]=(u*I-f*S+h*M)*P,e[8]=(o*L-a*O+l*_)*P,e[9]=(i*O-t*L-s*_)*P,e[10]=(g*w-y*S+m*b)*P,e[11]=(d*S-u*w-h*b)*P,e[12]=(a*C-o*D-c*_)*P,e[13]=(t*D-i*C+r*_)*P,e[14]=(y*M-g*A-p*b)*P,e[15]=(u*A-d*M+f*b)*P,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,d=a+a,f=s*l,h=s*u,g=s*d,y=o*u,p=o*d,m=a*d,b=c*l,M=c*u,S=c*d,A=i.x,w=i.y,I=i.z;return r[0]=(1-(y+m))*A,r[1]=(h+S)*A,r[2]=(g-M)*A,r[3]=0,r[4]=(h-S)*w,r[5]=(1-(f+m))*w,r[6]=(p+b)*w,r[7]=0,r[8]=(g+M)*I,r[9]=(p-b)*I,r[10]=(1-(f+y))*I,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=ra.set(r[0],r[1],r[2]).length(),a=ra.set(r[4],r[5],r[6]).length(),c=ra.set(r[8],r[9],r[10]).length();s<0&&(o=-o),Si.copy(this);let l=1/o,u=1/a,d=1/c;return Si.elements[0]*=l,Si.elements[1]*=l,Si.elements[2]*=l,Si.elements[4]*=u,Si.elements[5]*=u,Si.elements[6]*=u,Si.elements[8]*=d,Si.elements[9]*=d,Si.elements[10]*=d,t.setFromRotationMatrix(Si),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=Ti,c=!1){let l=this.elements,u=2*s/(t-e),d=2*s/(i-r),f=(t+e)/(t-e),h=(i+r)/(i-r),g,y;if(c)g=s/(o-s),y=o*s/(o-s);else if(a===Ti)g=-(o+s)/(o-s),y=-2*o*s/(o-s);else if(a===Ea)g=-o/(o-s),y=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=f,l[12]=0,l[1]=0,l[5]=d,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=y,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=Ti,c=!1){let l=this.elements,u=2/(t-e),d=2/(i-r),f=-(t+e)/(t-e),h=-(i+r)/(i-r),g,y;if(c)g=1/(o-s),y=o/(o-s);else if(a===Ti)g=-2/(o-s),y=-(o+s)/(o-s);else if(a===Ea)g=-1/(o-s),y=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=f,l[1]=0,l[5]=d,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=g,l[14]=y,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},ra=new T,Si=new Dt,$R=new T(0,0,0),qR=new T(1,1,1),ns=new T,nf=new T,Yn=new T,ZE=new Dt,KE=new pn,Ca=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],d=s[9],f=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(Be(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-Be(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-f,o),this._z=0);break;case"ZXY":this._x=Math.asin(Be(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-f,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-Be(f,-1,1)),Math.abs(f)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(Be(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-f,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-Be(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:Re("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return ZE.makeRotationFromQuaternion(t),this.setFromRotationMatrix(ZE,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return KE.setFromEuler(this),this.setFromQuaternion(KE,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Ta=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},XR=0,JE=new T,sa=new pn,Sr=new Dt,rf=new T,Vc=new T,YR=new T,ZR=new pn,QE=new T(1,0,0),eS=new T(0,1,0),tS=new T(0,0,1),nS={type:"added"},KR={type:"removed"},oa={type:"childadded",child:null},Uv={type:"childremoved",child:null},Jn=(()=>{class n extends Di{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:XR++}),this.uuid=Ar(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new T,i=new Ca,r=new pn,s=new T(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Dt},normalMatrix:{value:new je}}),this.matrix=new Dt,this.matrixWorld=new Dt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Ta,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return sa.setFromAxisAngle(t,i),this.quaternion.multiply(sa),this}rotateOnWorldAxis(t,i){return sa.setFromAxisAngle(t,i),this.quaternion.premultiply(sa),this}rotateX(t){return this.rotateOnAxis(QE,t)}rotateY(t){return this.rotateOnAxis(eS,t)}rotateZ(t){return this.rotateOnAxis(tS,t)}translateOnAxis(t,i){return JE.copy(t).applyQuaternion(this.quaternion),this.position.add(JE.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(QE,t)}translateY(t){return this.translateOnAxis(eS,t)}translateZ(t){return this.translateOnAxis(tS,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(Sr.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?rf.copy(t):rf.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Vc.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?Sr.lookAt(Vc,rf,this.up):Sr.lookAt(rf,Vc,this.up),this.quaternion.setFromRotationMatrix(Sr),s&&(Sr.extractRotation(s.matrixWorld),sa.setFromRotationMatrix(Sr),this.quaternion.premultiply(sa.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Pe("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(nS),oa.child=t,this.dispatchEvent(oa),oa.child=null):Pe("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(KR),Uv.child=t,this.dispatchEvent(Uv),Uv.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),Sr.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),Sr.multiply(t.parent.matrixWorld)),t.applyMatrix4(Sr),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(nS),oa.child=t,this.dispatchEvent(oa),oa.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Vc,t,YR),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Vc,ZR,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>ke(Ae({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>Ae({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let f=l[u];o(t.shapes,f)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),f=a(t.shapes),h=a(t.skeletons),g=a(t.animations),y=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),f.length>0&&(r.shapes=f),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),y.length>0&&(r.nodes=y)}return r.object=s,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.pivot=t.pivot!==null?t.pivot.clone():null,this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new T(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),di=class extends Jn{constructor(){super(),this.isGroup=!0,this.type="Group"}},JR={type:"move"},Da=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new di,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new di,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new T,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new T),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new di,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new T,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new T,this._grip.eventsEnabled=!1),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let y of e.hand.values()){let p=t.getJointPose(y,i),m=this._getHandJoint(l,y);p!==null&&(m.matrix.fromArray(p.transform.matrix),m.matrix.decompose(m.position,m.rotation,m.scale),m.matrixWorldNeedsUpdate=!0,m.jointRadius=p.radius),m.visible=p!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,g=.005;l.inputState.pinching&&f>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&f<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1,c.eventsEnabled&&c.dispatchEvent({type:"gripUpdated",data:e,target:this})));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(JR)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new di;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},sM={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},is={h:0,s:0,l:0},sf={h:0,s:0,l:0};function Bv(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var Ve=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Cn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,at.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=at.workingColorSpace){return this.r=e,this.g=t,this.b=i,at.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=at.workingColorSpace){if(e=ky(e,1),t=Be(t,0,1),i=Be(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=Bv(o,s,e+1/3),this.g=Bv(o,s,e),this.b=Bv(o,s,e-1/3)}return at.colorSpaceToWorking(this,r),this}setStyle(e,t=Cn){function i(s){s!==void 0&&parseFloat(s)<1&&Re("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:Re("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);Re("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Cn){let i=sM[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Re("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Ir(e.r),this.g=Ir(e.g),this.b=Ir(e.b),this}copyLinearToSRGB(e){return this.r=ba(e.r),this.g=ba(e.g),this.b=ba(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Cn){return at.workingToColorSpace(wn.copy(this),e),Math.round(Be(wn.r*255,0,255))*65536+Math.round(Be(wn.g*255,0,255))*256+Math.round(Be(wn.b*255,0,255))}getHexString(e=Cn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=at.workingColorSpace){at.workingToColorSpace(wn.copy(this),t);let i=wn.r,r=wn.g,s=wn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let d=o-a;switch(l=u<=.5?d/(o+a):d/(2-o-a),o){case i:c=(r-s)/d+(r<s?6:0);break;case r:c=(s-i)/d+2;break;case s:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=at.workingColorSpace){return at.workingToColorSpace(wn.copy(this),t),e.r=wn.r,e.g=wn.g,e.b=wn.b,e}getStyle(e=Cn){at.workingToColorSpace(wn.copy(this),e);let t=wn.r,i=wn.g,r=wn.b;return e!==Cn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(is),this.setHSL(is.h+e,is.s+t,is.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(is),e.getHSL(sf);let i=Yc(is.h,sf.h,t),r=Yc(is.s,sf.s,t),s=Yc(is.l,sf.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},wn=new Ve;Ve.NAMES=sM;var nl=class extends Jn{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new Ca,this.environmentIntensity=1,this.environmentRotation=new Ca,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},Mi=new T,Mr=new T,Vv=new T,wr=new T,aa=new T,ca=new T,iS=new T,Hv=new T,zv=new T,Gv=new T,jv=new it,Wv=new it,$v=new it,Dr=class n{constructor(e=new T,t=new T,i=new T){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Mi.subVectors(e,t),r.cross(Mi);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Mi.subVectors(r,t),Mr.subVectors(i,t),Vv.subVectors(e,t);let o=Mi.dot(Mi),a=Mi.dot(Mr),c=Mi.dot(Vv),l=Mr.dot(Mr),u=Mr.dot(Vv),d=o*l-a*a;if(d===0)return s.set(0,0,0),null;let f=1/d,h=(l*c-a*u)*f,g=(o*u-a*c)*f;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,wr)===null?!1:wr.x>=0&&wr.y>=0&&wr.x+wr.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,wr)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,wr.x),c.addScaledVector(o,wr.y),c.addScaledVector(a,wr.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return jv.setScalar(0),Wv.setScalar(0),$v.setScalar(0),jv.fromBufferAttribute(e,t),Wv.fromBufferAttribute(e,i),$v.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(jv,s.x),o.addScaledVector(Wv,s.y),o.addScaledVector($v,s.z),o}static isFrontFacing(e,t,i,r){return Mi.subVectors(i,t),Mr.subVectors(e,t),Mi.cross(Mr).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Mi.subVectors(this.c,this.b),Mr.subVectors(this.a,this.b),Mi.cross(Mr).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;aa.subVectors(r,i),ca.subVectors(s,i),Hv.subVectors(e,i);let c=aa.dot(Hv),l=ca.dot(Hv);if(c<=0&&l<=0)return t.copy(i);zv.subVectors(e,r);let u=aa.dot(zv),d=ca.dot(zv);if(u>=0&&d<=u)return t.copy(r);let f=c*d-u*l;if(f<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(aa,o);Gv.subVectors(e,s);let h=aa.dot(Gv),g=ca.dot(Gv);if(g>=0&&h<=g)return t.copy(s);let y=h*l-c*g;if(y<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(ca,a);let p=u*g-h*d;if(p<=0&&d-u>=0&&h-g>=0)return iS.subVectors(s,r),a=(d-u)/(d-u+(h-g)),t.copy(r).addScaledVector(iS,a);let m=1/(p+y+f);return o=y*m,a=f*m,t.copy(i).addScaledVector(aa,o).addScaledVector(ca,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},Dn=class{constructor(e=new T(1/0,1/0,1/0),t=new T(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(wi.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(wi.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=wi.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,wi):wi.fromBufferAttribute(s,o),wi.applyMatrix4(e.matrixWorld),this.expandByPoint(wi);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),of.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),of.copy(i.boundingBox)),of.applyMatrix4(e.matrixWorld),this.union(of)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,wi),wi.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(Hc),af.subVectors(this.max,Hc),la.subVectors(e.a,Hc),ua.subVectors(e.b,Hc),da.subVectors(e.c,Hc),rs.subVectors(ua,la),ss.subVectors(da,ua),eo.subVectors(la,da);let t=[0,-rs.z,rs.y,0,-ss.z,ss.y,0,-eo.z,eo.y,rs.z,0,-rs.x,ss.z,0,-ss.x,eo.z,0,-eo.x,-rs.y,rs.x,0,-ss.y,ss.x,0,-eo.y,eo.x,0];return!qv(t,la,ua,da,af)||(t=[1,0,0,0,1,0,0,0,1],!qv(t,la,ua,da,af))?!1:(cf.crossVectors(rs,ss),t=[cf.x,cf.y,cf.z],qv(t,la,ua,da,af))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,wi).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(wi).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Cr[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Cr[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Cr[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Cr[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Cr[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Cr[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Cr[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Cr[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Cr),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},Cr=[new T,new T,new T,new T,new T,new T,new T,new T],wi=new T,of=new Dn,la=new T,ua=new T,da=new T,rs=new T,ss=new T,eo=new T,Hc=new T,af=new T,cf=new T,to=new T;function qv(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){to.fromArray(n,s);let a=r.x*Math.abs(to.x)+r.y*Math.abs(to.y)+r.z*Math.abs(to.z),c=e.dot(to),l=t.dot(to),u=i.dot(to);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var Zt=new T,lf=new Te,QR=0,Hn=class extends Di{constructor(e,t,i=!1){if(super(),Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:QR++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=Gf,this.updateRanges=[],this.gpuType=Ri,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)lf.fromBufferAttribute(this,t),lf.applyMatrix3(e),this.setXY(t,lf.x,lf.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Zt.fromBufferAttribute(this,t),Zt.applyMatrix3(e),this.setXYZ(t,Zt.x,Zt.y,Zt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Zt.fromBufferAttribute(this,t),Zt.applyMatrix4(e),this.setXYZ(t,Zt.x,Zt.y,Zt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Zt.fromBufferAttribute(this,t),Zt.applyNormalMatrix(e),this.setXYZ(t,Zt.x,Zt.y,Zt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Zt.fromBufferAttribute(this,t),Zt.transformDirection(e),this.setXYZ(t,Zt.x,Zt.y,Zt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=Ci(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=gt(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=Ci(t,this.array)),t}setX(e,t){return this.normalized&&(t=gt(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=Ci(t,this.array)),t}setY(e,t){return this.normalized&&(t=gt(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=Ci(t,this.array)),t}setZ(e,t){return this.normalized&&(t=gt(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=Ci(t,this.array)),t}setW(e,t){return this.normalized&&(t=gt(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=gt(t,this.array),i=gt(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=gt(t,this.array),i=gt(i,this.array),r=gt(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=gt(t,this.array),i=gt(i,this.array),r=gt(r,this.array),s=gt(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==Gf&&(e.usage=this.usage),e}dispose(){this.dispatchEvent({type:"dispose"})}};var il=class extends Hn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var rl=class extends Hn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var ut=class extends Hn{constructor(e,t,i){super(new Float32Array(e),t,i)}},eN=new Dn,zc=new T,Xv=new T,Qn=class{constructor(e=new T,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):eN.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;zc.subVectors(e,this.center);let t=zc.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(zc,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Xv.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(zc.copy(e.center).add(Xv)),this.expandByPoint(zc.copy(e.center).sub(Xv))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},tN=0,ui=new Dt,Yv=new Jn,fa=new T,Zn=new Dn,Gc=new Dn,on=new T,Wt=class n extends Di{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:tN++}),this.uuid=Ar(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(wR(e)?rl:il)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new je().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return ui.makeRotationFromQuaternion(e),this.applyMatrix4(ui),this}rotateX(e){return ui.makeRotationX(e),this.applyMatrix4(ui),this}rotateY(e){return ui.makeRotationY(e),this.applyMatrix4(ui),this}rotateZ(e){return ui.makeRotationZ(e),this.applyMatrix4(ui),this}translate(e,t,i){return ui.makeTranslation(e,t,i),this.applyMatrix4(ui),this}scale(e,t,i){return ui.makeScale(e,t,i),this.applyMatrix4(ui),this}lookAt(e){return Yv.lookAt(e),Yv.updateMatrix(),this.applyMatrix4(Yv.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(fa).negate(),this.translate(fa.x,fa.y,fa.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new ut(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&Re("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Dn);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Pe("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new T(-1/0,-1/0,-1/0),new T(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];Zn.setFromBufferAttribute(s),this.morphTargetsRelative?(on.addVectors(this.boundingBox.min,Zn.min),this.boundingBox.expandByPoint(on),on.addVectors(this.boundingBox.max,Zn.max),this.boundingBox.expandByPoint(on)):(this.boundingBox.expandByPoint(Zn.min),this.boundingBox.expandByPoint(Zn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Pe('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Qn);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Pe("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new T,1/0);return}if(e){let i=this.boundingSphere.center;if(Zn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];Gc.setFromBufferAttribute(a),this.morphTargetsRelative?(on.addVectors(Zn.min,Gc.min),Zn.expandByPoint(on),on.addVectors(Zn.max,Gc.max),Zn.expandByPoint(on)):(Zn.expandByPoint(Gc.min),Zn.expandByPoint(Gc.max))}Zn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)on.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(on));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)on.fromBufferAttribute(a,l),c&&(fa.fromBufferAttribute(e,l),on.add(fa)),r=Math.max(r,i.distanceToSquared(on))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Pe('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Pe("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new Hn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let _=0;_<i.count;_++)a[_]=new T,c[_]=new T;let l=new T,u=new T,d=new T,f=new Te,h=new Te,g=new Te,y=new T,p=new T;function m(_,C,O){l.fromBufferAttribute(i,_),u.fromBufferAttribute(i,C),d.fromBufferAttribute(i,O),f.fromBufferAttribute(s,_),h.fromBufferAttribute(s,C),g.fromBufferAttribute(s,O),u.sub(l),d.sub(l),h.sub(f),g.sub(f);let D=1/(h.x*g.y-g.x*h.y);isFinite(D)&&(y.copy(u).multiplyScalar(g.y).addScaledVector(d,-h.y).multiplyScalar(D),p.copy(d).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(D),a[_].add(y),a[C].add(y),a[O].add(y),c[_].add(p),c[C].add(p),c[O].add(p))}let b=this.groups;b.length===0&&(b=[{start:0,count:e.count}]);for(let _=0,C=b.length;_<C;++_){let O=b[_],D=O.start,L=O.count;for(let G=D,W=D+L;G<W;G+=3)m(e.getX(G+0),e.getX(G+1),e.getX(G+2))}let M=new T,S=new T,A=new T,w=new T;function I(_){A.fromBufferAttribute(r,_),w.copy(A);let C=a[_];M.copy(C),M.sub(A.multiplyScalar(A.dot(C))).normalize(),S.crossVectors(w,C);let D=S.dot(c[_])<0?-1:1;o.setXYZW(_,M.x,M.y,M.z,D)}for(let _=0,C=b.length;_<C;++_){let O=b[_],D=O.start,L=O.count;for(let G=D,W=D+L;G<W;G+=3)I(e.getX(G+0)),I(e.getX(G+1)),I(e.getX(G+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new Hn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new T,s=new T,o=new T,a=new T,c=new T,l=new T,u=new T,d=new T;if(e)for(let f=0,h=e.count;f<h;f+=3){let g=e.getX(f+0),y=e.getX(f+1),p=e.getX(f+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,y),o.fromBufferAttribute(t,p),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,y),l.fromBufferAttribute(i,p),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(y,c.x,c.y,c.z),i.setXYZ(p,l.x,l.y,l.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),s.fromBufferAttribute(t,f+1),o.fromBufferAttribute(t,f+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)on.fromBufferAttribute(e,t),on.normalize(),e.setXYZ(t,on.x,on.y,on.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,f=new l.constructor(c.length*u),h=0,g=0;for(let y=0,p=c.length;y<p;y++){a.isInterleavedBufferAttribute?h=c[y]*a.data.stride+a.offset:h=c[y]*u;for(let m=0;m<u;m++)f[g++]=l[h++]}return new Hn(f,u,d)}if(this.index===null)return Re("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,d=l.length;u<d;u++){let f=l[u],h=e(f,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,f=l.length;d<f;d++){let h=l[d];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],d=s[l];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let d=o[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},sl=class{constructor(e,t){this.isInterleavedBuffer=!0,this.array=e,this.stride=t,this.count=e!==void 0?e.length/t:0,this.usage=Gf,this.updateRanges=[],this.version=0,this.uuid=Ar()}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.array=new e.array.constructor(e.array),this.count=e.count,this.stride=e.stride,this.usage=e.usage,this}copyAt(e,t,i){e*=this.stride,i*=t.stride;for(let r=0,s=this.stride;r<s;r++)this.array[e+r]=t.array[i+r];return this}set(e,t=0){return this.array.set(e,t),this}clone(e){e.arrayBuffers===void 0&&(e.arrayBuffers={}),this.array.buffer._uuid===void 0&&(this.array.buffer._uuid=Ar()),e.arrayBuffers[this.array.buffer._uuid]===void 0&&(e.arrayBuffers[this.array.buffer._uuid]=this.array.slice(0).buffer);let t=new this.array.constructor(e.arrayBuffers[this.array.buffer._uuid]),i=new this.constructor(t,this.stride);return i.setUsage(this.usage),i}onUpload(e){return this.onUploadCallback=e,this}toJSON(e){return e.arrayBuffers===void 0&&(e.arrayBuffers={}),this.array.buffer._uuid===void 0&&(this.array.buffer._uuid=Ar()),e.arrayBuffers[this.array.buffer._uuid]===void 0&&(e.arrayBuffers[this.array.buffer._uuid]=Array.from(new Uint32Array(this.array.buffer))),{uuid:this.uuid,buffer:this.array.buffer._uuid,type:this.array.constructor.name,stride:this.stride}}},Rn=new T,ei=class n{constructor(e,t,i,r=!1){this.isInterleavedBufferAttribute=!0,this.name="",this.data=e,this.itemSize=t,this.offset=i,this.normalized=r}get count(){return this.data.count}get array(){return this.data.array}set needsUpdate(e){this.data.needsUpdate=e}applyMatrix4(e){for(let t=0,i=this.data.count;t<i;t++)Rn.fromBufferAttribute(this,t),Rn.applyMatrix4(e),this.setXYZ(t,Rn.x,Rn.y,Rn.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Rn.fromBufferAttribute(this,t),Rn.applyNormalMatrix(e),this.setXYZ(t,Rn.x,Rn.y,Rn.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Rn.fromBufferAttribute(this,t),Rn.transformDirection(e),this.setXYZ(t,Rn.x,Rn.y,Rn.z);return this}getComponent(e,t){let i=this.array[e*this.data.stride+this.offset+t];return this.normalized&&(i=Ci(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=gt(i,this.array)),this.data.array[e*this.data.stride+this.offset+t]=i,this}setX(e,t){return this.normalized&&(t=gt(t,this.array)),this.data.array[e*this.data.stride+this.offset]=t,this}setY(e,t){return this.normalized&&(t=gt(t,this.array)),this.data.array[e*this.data.stride+this.offset+1]=t,this}setZ(e,t){return this.normalized&&(t=gt(t,this.array)),this.data.array[e*this.data.stride+this.offset+2]=t,this}setW(e,t){return this.normalized&&(t=gt(t,this.array)),this.data.array[e*this.data.stride+this.offset+3]=t,this}getX(e){let t=this.data.array[e*this.data.stride+this.offset];return this.normalized&&(t=Ci(t,this.array)),t}getY(e){let t=this.data.array[e*this.data.stride+this.offset+1];return this.normalized&&(t=Ci(t,this.array)),t}getZ(e){let t=this.data.array[e*this.data.stride+this.offset+2];return this.normalized&&(t=Ci(t,this.array)),t}getW(e){let t=this.data.array[e*this.data.stride+this.offset+3];return this.normalized&&(t=Ci(t,this.array)),t}setXY(e,t,i){return e=e*this.data.stride+this.offset,this.normalized&&(t=gt(t,this.array),i=gt(i,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this}setXYZ(e,t,i,r){return e=e*this.data.stride+this.offset,this.normalized&&(t=gt(t,this.array),i=gt(i,this.array),r=gt(r,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this.data.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e=e*this.data.stride+this.offset,this.normalized&&(t=gt(t,this.array),i=gt(i,this.array),r=gt(r,this.array),s=gt(s,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this.data.array[e+2]=r,this.data.array[e+3]=s,this}clone(e){if(e===void 0){el("InterleavedBufferAttribute.clone(): Cloning an interleaved buffer attribute will de-interleave buffer data.");let t=[];for(let i=0;i<this.count;i++){let r=i*this.data.stride+this.offset;for(let s=0;s<this.itemSize;s++)t.push(this.data.array[r+s])}return new Hn(new this.array.constructor(t),this.itemSize,this.normalized)}else return e.interleavedBuffers===void 0&&(e.interleavedBuffers={}),e.interleavedBuffers[this.data.uuid]===void 0&&(e.interleavedBuffers[this.data.uuid]=this.data.clone(e)),new n(e.interleavedBuffers[this.data.uuid],this.itemSize,this.offset,this.normalized)}toJSON(e){if(e===void 0){el("InterleavedBufferAttribute.toJSON(): Serializing an interleaved buffer attribute will de-interleave buffer data.");let t=[];for(let i=0;i<this.count;i++){let r=i*this.data.stride+this.offset;for(let s=0;s<this.itemSize;s++)t.push(this.data.array[r+s])}return{itemSize:this.itemSize,type:this.array.constructor.name,array:t,normalized:this.normalized}}else return e.interleavedBuffers===void 0&&(e.interleavedBuffers={}),e.interleavedBuffers[this.data.uuid]===void 0&&(e.interleavedBuffers[this.data.uuid]=this.data.toJSON(e)),{isInterleavedBufferAttribute:!0,itemSize:this.itemSize,data:this.data.uuid,offset:this.offset,normalized:this.normalized}}},nN=0,er=class extends Di{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:nN++}),this.uuid=Ar(),this.name="",this.type="Material",this.blending=so,this.side=Rr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Rf,this.blendDst=Nf,this.blendEquation=cs,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new Ve(0,0,0),this.blendAlpha=0,this.depthFunc=oo,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=dy,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=ro,this.stencilZFail=ro,this.stencilZPass=ro,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Re(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Re(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==so&&(i.blending=this.blending),this.side!==Rr&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Rf&&(i.blendSrc=this.blendSrc),this.blendDst!==Nf&&(i.blendDst=this.blendDst),this.blendEquation!==cs&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==oo&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==dy&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==ro&&(i.stencilFail=this.stencilFail),this.stencilZFail!==ro&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==ro&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}},Aa=class extends er{constructor(e){super(),this.isSpriteMaterial=!0,this.type="SpriteMaterial",this.color=new Ve(16777215),this.map=null,this.alphaMap=null,this.rotation=0,this.sizeAttenuation=!0,this.transparent=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.rotation=e.rotation,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},ha,jc=new T,pa=new T,ma=new T,ga=new Te,Wc=new Te,oM=new Dt,uf=new T,$c=new T,df=new T,rS=new Te,Zv=new Te,sS=new Te,ol=class extends Jn{constructor(e=new Aa){if(super(),this.isSprite=!0,this.type="Sprite",ha===void 0){ha=new Wt;let t=new Float32Array([-.5,-.5,0,0,0,.5,-.5,0,1,0,.5,.5,0,1,1,-.5,.5,0,0,1]),i=new sl(t,5);ha.setIndex([0,1,2,0,2,3]),ha.setAttribute("position",new ei(i,3,0,!1)),ha.setAttribute("uv",new ei(i,2,3,!1))}this.geometry=ha,this.material=e,this.center=new Te(.5,.5),this.count=1}raycast(e,t){e.camera===null&&Pe('Sprite: "Raycaster.camera" needs to be set in order to raycast against sprites.'),pa.setFromMatrixScale(this.matrixWorld),oM.copy(e.camera.matrixWorld),this.modelViewMatrix.multiplyMatrices(e.camera.matrixWorldInverse,this.matrixWorld),ma.setFromMatrixPosition(this.modelViewMatrix),e.camera.isPerspectiveCamera&&this.material.sizeAttenuation===!1&&pa.multiplyScalar(-ma.z);let i=this.material.rotation,r,s;i!==0&&(s=Math.cos(i),r=Math.sin(i));let o=this.center;ff(uf.set(-.5,-.5,0),ma,o,pa,r,s),ff($c.set(.5,-.5,0),ma,o,pa,r,s),ff(df.set(.5,.5,0),ma,o,pa,r,s),rS.set(0,0),Zv.set(1,0),sS.set(1,1);let a=e.ray.intersectTriangle(uf,$c,df,!1,jc);if(a===null&&(ff($c.set(-.5,.5,0),ma,o,pa,r,s),Zv.set(0,1),a=e.ray.intersectTriangle(uf,df,$c,!1,jc),a===null))return;let c=e.ray.origin.distanceTo(jc);c<e.near||c>e.far||t.push({distance:c,point:jc.clone(),uv:Dr.getInterpolation(jc,uf,$c,df,rS,Zv,sS,new Te),face:null,object:this})}copy(e,t){return super.copy(e,t),e.center!==void 0&&this.center.copy(e.center),this.material=e.material,this}};function ff(n,e,t,i,r,s){ga.subVectors(n,t).addScalar(.5).multiply(i),r!==void 0?(Wc.x=s*ga.x-r*ga.y,Wc.y=r*ga.x+s*ga.y):Wc.copy(ga),n.copy(e),n.x+=Wc.x,n.y+=Wc.y,n.applyMatrix4(oM)}var Tr=new T,Kv=new T,hf=new T,os=new T,Jv=new T,pf=new T,Qv=new T,ls=class{constructor(e=new T,t=new T(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Tr)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Tr.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Tr.copy(this.origin).addScaledVector(this.direction,t),Tr.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Kv.copy(e).add(t).multiplyScalar(.5),hf.copy(t).sub(e).normalize(),os.copy(this.origin).sub(Kv);let s=e.distanceTo(t)*.5,o=-this.direction.dot(hf),a=os.dot(this.direction),c=-os.dot(hf),l=os.lengthSq(),u=Math.abs(1-o*o),d,f,h,g;if(u>0)if(d=o*c-a,f=o*a-c,g=s*u,d>=0)if(f>=-g)if(f<=g){let y=1/u;d*=y,f*=y,h=d*(d+o*f+2*a)+f*(o*d+f+2*c)+l}else f=s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f=-s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f<=-g?(d=Math.max(0,-(-o*s+a)),f=d>0?-s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l):f<=g?(d=0,f=Math.min(Math.max(-s,-c),s),h=f*(f+2*c)+l):(d=Math.max(0,-(o*s+a)),f=d>0?s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l);else f=o>0?-s:s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Kv).addScaledVector(hf,f),h}intersectSphere(e,t){Tr.subVectors(e.center,this.origin);let i=Tr.dot(this.direction),r=Tr.dot(Tr)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return l>=0?(i=(e.min.x-f.x)*l,r=(e.max.x-f.x)*l):(i=(e.max.x-f.x)*l,r=(e.min.x-f.x)*l),u>=0?(s=(e.min.y-f.y)*u,o=(e.max.y-f.y)*u):(s=(e.max.y-f.y)*u,o=(e.min.y-f.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-f.z)*d,c=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,c=(e.min.z-f.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Tr)!==null}intersectTriangle(e,t,i,r,s){Jv.subVectors(t,e),pf.subVectors(i,e),Qv.crossVectors(Jv,pf);let o=this.direction.dot(Qv),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;os.subVectors(this.origin,e);let c=a*this.direction.dot(pf.crossVectors(os,pf));if(c<0)return null;let l=a*this.direction.dot(Jv.cross(os));if(l<0||c+l>o)return null;let u=-a*os.dot(Qv);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},us=class extends er{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new Ve(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Ca,this.combine=by,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},oS=new Dt,no=new ls,mf=new Qn,aS=new T,gf=new T,vf=new T,yf=new T,ey=new T,_f=new T,cS=new T,xf=new T,tn=class extends Jn{constructor(e=new Wt,t=new us){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){_f.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],d=s[c];u!==0&&(ey.fromBufferAttribute(d,e),o?_f.addScaledVector(ey,u):_f.addScaledVector(ey.sub(t),u))}t.add(_f)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),mf.copy(i.boundingSphere),mf.applyMatrix4(s),no.copy(e.ray).recast(e.near),!(mf.containsPoint(no.origin)===!1&&(no.intersectSphere(mf,aS)===null||no.origin.distanceToSquared(aS)>(e.far-e.near)**2))&&(oS.copy(s).invert(),no.copy(e.ray).applyMatrix4(oS),!(i.boundingBox!==null&&no.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,no)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,f=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,y=f.length;g<y;g++){let p=f[g],m=o[p.materialIndex],b=Math.max(p.start,h.start),M=Math.min(a.count,Math.min(p.start+p.count,h.start+h.count));for(let S=b,A=M;S<A;S+=3){let w=a.getX(S),I=a.getX(S+1),_=a.getX(S+2);r=bf(this,m,e,i,l,u,d,w,I,_),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=p.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),y=Math.min(a.count,h.start+h.count);for(let p=g,m=y;p<m;p+=3){let b=a.getX(p),M=a.getX(p+1),S=a.getX(p+2);r=bf(this,o,e,i,l,u,d,b,M,S),r&&(r.faceIndex=Math.floor(p/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,y=f.length;g<y;g++){let p=f[g],m=o[p.materialIndex],b=Math.max(p.start,h.start),M=Math.min(c.count,Math.min(p.start+p.count,h.start+h.count));for(let S=b,A=M;S<A;S+=3){let w=S,I=S+1,_=S+2;r=bf(this,m,e,i,l,u,d,w,I,_),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=p.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),y=Math.min(c.count,h.start+h.count);for(let p=g,m=y;p<m;p+=3){let b=p,M=p+1,S=p+2;r=bf(this,o,e,i,l,u,d,b,M,S),r&&(r.faceIndex=Math.floor(p/3),t.push(r))}}}};function iN(n,e,t,i,r,s,o,a){let c;if(e.side===Pn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===Rr,a),c===null)return null;xf.copy(a),xf.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(xf);return l<t.near||l>t.far?null:{distance:l,point:xf.clone(),object:n}}function bf(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,gf),n.getVertexPosition(c,vf),n.getVertexPosition(l,yf);let u=iN(n,e,t,i,gf,vf,yf,cS);if(u){let d=new T;Dr.getBarycoord(cS,gf,vf,yf,d),r&&(u.uv=Dr.getInterpolatedAttribute(r,a,c,l,d,new Te)),s&&(u.uv1=Dr.getInterpolatedAttribute(s,a,c,l,d,new Te)),o&&(u.normal=Dr.getInterpolatedAttribute(o,a,c,l,d,new T),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:c,c:l,normal:new T,materialIndex:0};Dr.getNormal(gf,vf,yf,f.normal),u.face=f,u.barycoord=d}return u}var Xf=class extends ir{constructor(e=null,t=1,i=1,r,s,o,a,c,l=an,u=an,d,f){super(null,o,a,c,l,u,r,s,d,f),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var ty=new T,rN=new T,sN=new je,Vn=class{constructor(e=new T(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=ty.subVectors(i,t).cross(rN.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t,i=!0){let r=e.delta(ty),s=this.normal.dot(r);if(s===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/s;return i===!0&&(o<0||o>1)?null:t.copy(e.start).addScaledVector(r,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||sN.getNormalMatrix(e),r=this.coplanarPoint(ty).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},io=new Qn,oN=new Te(.5,.5),Ef=new T,Ia=class{constructor(e=new Vn,t=new Vn,i=new Vn,r=new Vn,s=new Vn,o=new Vn){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=Ti,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],d=s[5],f=s[6],h=s[7],g=s[8],y=s[9],p=s[10],m=s[11],b=s[12],M=s[13],S=s[14],A=s[15];if(r[0].setComponents(l-o,h-u,m-g,A-b).normalize(),r[1].setComponents(l+o,h+u,m+g,A+b).normalize(),r[2].setComponents(l+a,h+d,m+y,A+M).normalize(),r[3].setComponents(l-a,h-d,m-y,A-M).normalize(),i)r[4].setComponents(c,f,p,S).normalize(),r[5].setComponents(l-c,h-f,m-p,A-S).normalize();else if(r[4].setComponents(l-c,h-f,m-p,A-S).normalize(),t===Ti)r[5].setComponents(l+c,h+f,m+p,A+S).normalize();else if(t===Ea)r[5].setComponents(c,f,p,S).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),io.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),io.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(io)}intersectsSprite(e){io.center.set(0,0,0);let t=oN.distanceTo(e.center);return io.radius=.7071067811865476+t,io.applyMatrix4(e.matrixWorld),this.intersectsSphere(io)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Ef.x=r.normal.x>0?e.max.x:e.min.x,Ef.y=r.normal.y>0?e.max.y:e.min.y,Ef.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Ef)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var ds=class extends er{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new Ve(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},Yf=new T,Zf=new T,lS=new Dt,qc=new ls,Sf=new Qn,ny=new T,uS=new T,al=class extends Jn{constructor(e=new Wt,t=new ds){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,s=t.count;r<s;r++)Yf.fromBufferAttribute(t,r-1),Zf.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=Yf.distanceTo(Zf);e.setAttribute("lineDistance",new ut(i,1))}else Re("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Line.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),Sf.copy(i.boundingSphere),Sf.applyMatrix4(r),Sf.radius+=s,e.ray.intersectsSphere(Sf)===!1)return;lS.copy(r).invert(),qc.copy(e.ray).applyMatrix4(lS);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,f=i.attributes.position;if(u!==null){let h=Math.max(0,o.start),g=Math.min(u.count,o.start+o.count);for(let y=h,p=g-1;y<p;y+=l){let m=u.getX(y),b=u.getX(y+1),M=Mf(this,e,qc,c,m,b,y);M&&t.push(M)}if(this.isLineLoop){let y=u.getX(g-1),p=u.getX(h),m=Mf(this,e,qc,c,y,p,g-1);m&&t.push(m)}}else{let h=Math.max(0,o.start),g=Math.min(f.count,o.start+o.count);for(let y=h,p=g-1;y<p;y+=l){let m=Mf(this,e,qc,c,y,y+1,y);m&&t.push(m)}if(this.isLineLoop){let y=Mf(this,e,qc,c,g-1,h,g-1);y&&t.push(y)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function Mf(n,e,t,i,r,s,o){let a=n.geometry.attributes.position;if(Yf.fromBufferAttribute(a,r),Zf.fromBufferAttribute(a,s),t.distanceSqToSegment(Yf,Zf,ny,uS)>i)return;ny.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(ny);if(!(l<e.near||l>e.far))return{distance:l,point:uS.clone().applyMatrix4(n.matrixWorld),index:o,face:null,faceIndex:null,barycoord:null,object:n}}var dS=new T,fS=new T,Ra=class extends al{constructor(e,t){super(e,t),this.isLineSegments=!0,this.type="LineSegments"}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[];for(let r=0,s=t.count;r<s;r+=2)dS.fromBufferAttribute(t,r),fS.fromBufferAttribute(t,r+1),i[r]=r===0?0:i[r-1],i[r+1]=i[r]+dS.distanceTo(fS);e.setAttribute("lineDistance",new ut(i,1))}else Re("LineSegments.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}};var cl=class extends ir{constructor(e=[],t=ys,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},ll=class extends ir{constructor(e,t,i,r,s,o,a,c,l){super(e,t,i,r,s,o,a,c,l),this.isCanvasTexture=!0,this.needsUpdate=!0}};var Nr=class extends ir{constructor(e,t,i=Ii,r,s,o,a=an,c=an,l,u=Qi,d=1){if(u!==Qi&&u!==xs)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let f={width:e,height:t,depth:d};super(f,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new wa(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Kf=class extends Nr{constructor(e,t=Ii,i=ys,r,s,o=an,a=an,c,l=Qi){let u={width:e,height:e,depth:1},d=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=d,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},ul=class extends ir{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},Na=class n extends Wt{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],d=[],f=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new ut(l,3)),this.setAttribute("normal",new ut(u,3)),this.setAttribute("uv",new ut(d,2));function g(y,p,m,b,M,S,A,w,I,_,C){let O=S/I,D=A/_,L=S/2,G=A/2,W=w/2,P=I+1,H=_+1,U=0,J=0,Q=new T;for(let ce=0;ce<H;ce++){let xe=ce*D-G;for(let Se=0;Se<P;Se++){let rt=Se*O-L;Q[y]=rt*b,Q[p]=xe*M,Q[m]=W,l.push(Q.x,Q.y,Q.z),Q[y]=0,Q[p]=0,Q[m]=w>0?1:-1,u.push(Q.x,Q.y,Q.z),d.push(Se/I),d.push(1-ce/_),U+=1}}for(let ce=0;ce<_;ce++)for(let xe=0;xe<I;xe++){let Se=f+xe+P*ce,rt=f+xe+P*(ce+1),Ke=f+(xe+1)+P*(ce+1),Fe=f+(xe+1)+P*ce;c.push(Se,rt,Fe),c.push(rt,Ke,Fe),J+=6}a.addGroup(h,J,C),h+=J,f+=U}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var Jf=class n extends Wt{constructor(e=1,t=1,i=1,r=32,s=1,o=!1,a=0,c=Math.PI*2){super(),this.type="CylinderGeometry",this.parameters={radiusTop:e,radiusBottom:t,height:i,radialSegments:r,heightSegments:s,openEnded:o,thetaStart:a,thetaLength:c};let l=this;r=Math.floor(r),s=Math.floor(s);let u=[],d=[],f=[],h=[],g=0,y=[],p=i/2,m=0;b(),o===!1&&(e>0&&M(!0),t>0&&M(!1)),this.setIndex(u),this.setAttribute("position",new ut(d,3)),this.setAttribute("normal",new ut(f,3)),this.setAttribute("uv",new ut(h,2));function b(){let S=new T,A=new T,w=0,I=(t-e)/i;for(let _=0;_<=s;_++){let C=[],O=_/s,D=O*(t-e)+e;for(let L=0;L<=r;L++){let G=L/r,W=G*c+a,P=Math.sin(W),H=Math.cos(W);A.x=D*P,A.y=-O*i+p,A.z=D*H,d.push(A.x,A.y,A.z),S.set(P,I,H).normalize(),f.push(S.x,S.y,S.z),h.push(G,1-O),C.push(g++)}y.push(C)}for(let _=0;_<r;_++)for(let C=0;C<s;C++){let O=y[C][_],D=y[C+1][_],L=y[C+1][_+1],G=y[C][_+1];(e>0||C!==0)&&(u.push(O,D,G),w+=3),(t>0||C!==s-1)&&(u.push(D,L,G),w+=3)}l.addGroup(m,w,0),m+=w}function M(S){let A=g,w=new Te,I=new T,_=0,C=S===!0?e:t,O=S===!0?1:-1;for(let L=1;L<=r;L++)d.push(0,p*O,0),f.push(0,O,0),h.push(.5,.5),g++;let D=g;for(let L=0;L<=r;L++){let W=L/r*c+a,P=Math.cos(W),H=Math.sin(W);I.x=C*H,I.y=p*O,I.z=C*P,d.push(I.x,I.y,I.z),f.push(0,O,0),w.x=P*.5+.5,w.y=H*.5*O+.5,h.push(w.x,w.y),g++}for(let L=0;L<r;L++){let G=A+L,W=D+L;S===!0?u.push(W,W+1,G):u.push(W+1,W,G),_+=3}l.addGroup(m,_,S===!0?1:2),m+=_}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radiusTop,e.radiusBottom,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}},Qf=class n extends Jf{constructor(e=1,t=1,i=32,r=1,s=!1,o=0,a=Math.PI*2){super(0,e,t,i,r,s,o,a),this.type="ConeGeometry",this.parameters={radius:e,height:t,radialSegments:i,heightSegments:r,openEnded:s,thetaStart:o,thetaLength:a}}static fromJSON(e){return new n(e.radius,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}};var dl=class n extends Wt{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,f=t/c,h=[],g=[],y=[],p=[];for(let m=0;m<u;m++){let b=m*f-o;for(let M=0;M<l;M++){let S=M*d-s;g.push(S,-b,0),y.push(0,0,1),p.push(M/a),p.push(1-m/c)}}for(let m=0;m<c;m++)for(let b=0;b<a;b++){let M=b+l*m,S=b+l*(m+1),A=b+1+l*(m+1),w=b+1+l*m;h.push(M,S,w),h.push(S,A,w)}this.setIndex(h),this.setAttribute("position",new ut(g,3)),this.setAttribute("normal",new ut(y,3)),this.setAttribute("uv",new ut(p,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};var Pa=class n extends Wt{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let c=Math.min(o+a,Math.PI),l=0,u=[],d=new T,f=new T,h=[],g=[],y=[],p=[];for(let m=0;m<=i;m++){let b=[],M=m/i,S=0;m===0&&o===0?S=.5/t:m===i&&c===Math.PI&&(S=-.5/t);for(let A=0;A<=t;A++){let w=A/t;d.x=-e*Math.cos(r+w*s)*Math.sin(o+M*a),d.y=e*Math.cos(o+M*a),d.z=e*Math.sin(r+w*s)*Math.sin(o+M*a),g.push(d.x,d.y,d.z),f.copy(d).normalize(),y.push(f.x,f.y,f.z),p.push(w+S,1-M),b.push(l++)}u.push(b)}for(let m=0;m<i;m++)for(let b=0;b<t;b++){let M=u[m][b+1],S=u[m][b],A=u[m+1][b],w=u[m+1][b+1];(m!==0||o>0)&&h.push(M,S,w),(m!==i-1||c<Math.PI)&&h.push(S,A,w)}this.setIndex(h),this.setAttribute("position",new ut(g,3)),this.setAttribute("normal",new ut(y,3)),this.setAttribute("uv",new ut(p,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}};var fl=class extends Wt{constructor(e=null){if(super(),this.type="WireframeGeometry",this.parameters={geometry:e},e!==null){let t=[],i=new Set,r=new T,s=new T;if(e.index!==null){let o=e.attributes.position,a=e.index,c=e.groups;c.length===0&&(c=[{start:0,count:a.count,materialIndex:0}]);for(let l=0,u=c.length;l<u;++l){let d=c[l],f=d.start,h=d.count;for(let g=f,y=f+h;g<y;g+=3)for(let p=0;p<3;p++){let m=a.getX(g+p),b=a.getX(g+(p+1)%3);r.fromBufferAttribute(o,m),s.fromBufferAttribute(o,b),hS(r,s,i)===!0&&(t.push(r.x,r.y,r.z),t.push(s.x,s.y,s.z))}}}else{let o=e.attributes.position;for(let a=0,c=o.count/3;a<c;a++)for(let l=0;l<3;l++){let u=3*a+l,d=3*a+(l+1)%3;r.fromBufferAttribute(o,u),s.fromBufferAttribute(o,d),hS(r,s,i)===!0&&(t.push(r.x,r.y,r.z),t.push(s.x,s.y,s.z))}}this.setAttribute("position",new ut(t,3))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}};function hS(n,e,t){let i=`${n.x},${n.y},${n.z}-${e.x},${e.y},${e.z}`,r=`${e.x},${e.y},${e.z}-${n.x},${n.y},${n.z}`;return t.has(i)===!0||t.has(r)===!0?!1:(t.add(i),t.add(r),!0)}function co(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];if(pS(r))r.isRenderTargetTexture?(Re("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone();else if(Array.isArray(r))if(pS(r[0])){let s=[];for(let o=0,a=r.length;o<a;o++)s[o]=r[o].clone();e[t][i]=s}else e[t][i]=r.slice();else e[t][i]=r}}return e}function An(n){let e={};for(let t=0;t<n.length;t++){let i=co(n[t]);for(let r in i)e[r]=i[r]}return e}function pS(n){return n&&(n.isColor||n.isMatrix3||n.isMatrix4||n.isVector2||n.isVector3||n.isVector4||n.isTexture||n.isQuaternion)}function aN(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function Uy(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:at.workingColorSpace}var Nl={clone:co,merge:An},cN=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,lN=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Nn=class extends er{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=cN,this.fragmentShader=lN,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=co(e.uniforms),this.uniformsGroups=aN(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},eh=class extends Nn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}},th=class extends er{constructor(e){super(),this.isMeshStandardMaterial=!0,this.type="MeshStandardMaterial",this.defines={STANDARD:""},this.color=new Ve(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new Ve(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=Qh,this.normalScale=new Te(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Ca,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}},Oa=class extends th{constructor(e){super(),this.isMeshPhysicalMaterial=!0,this.defines={STANDARD:"",PHYSICAL:""},this.type="MeshPhysicalMaterial",this.anisotropyRotation=0,this.anisotropyMap=null,this.clearcoatMap=null,this.clearcoatRoughness=0,this.clearcoatRoughnessMap=null,this.clearcoatNormalScale=new Te(1,1),this.clearcoatNormalMap=null,this.ior=1.5,Object.defineProperty(this,"reflectivity",{get:function(){return Be(2.5*(this.ior-1)/(this.ior+1),0,1)},set:function(t){this.ior=(1+.4*t)/(1-.4*t)}}),this.iridescenceMap=null,this.iridescenceIOR=1.3,this.iridescenceThicknessRange=[100,400],this.iridescenceThicknessMap=null,this.sheenColor=new Ve(0),this.sheenColorMap=null,this.sheenRoughness=1,this.sheenRoughnessMap=null,this.transmissionMap=null,this.thickness=0,this.thicknessMap=null,this.attenuationDistance=1/0,this.attenuationColor=new Ve(1,1,1),this.specularIntensity=1,this.specularIntensityMap=null,this.specularColor=new Ve(1,1,1),this.specularColorMap=null,this._anisotropy=0,this._clearcoat=0,this._dispersion=0,this._iridescence=0,this._sheen=0,this._transmission=0,this.setValues(e)}get anisotropy(){return this._anisotropy}set anisotropy(e){this._anisotropy>0!=e>0&&this.version++,this._anisotropy=e}get clearcoat(){return this._clearcoat}set clearcoat(e){this._clearcoat>0!=e>0&&this.version++,this._clearcoat=e}get iridescence(){return this._iridescence}set iridescence(e){this._iridescence>0!=e>0&&this.version++,this._iridescence=e}get dispersion(){return this._dispersion}set dispersion(e){this._dispersion>0!=e>0&&this.version++,this._dispersion=e}get sheen(){return this._sheen}set sheen(e){this._sheen>0!=e>0&&this.version++,this._sheen=e}get transmission(){return this._transmission}set transmission(e){this._transmission>0!=e>0&&this.version++,this._transmission=e}copy(e){return super.copy(e),this.defines={STANDARD:"",PHYSICAL:""},this.anisotropy=e.anisotropy,this.anisotropyRotation=e.anisotropyRotation,this.anisotropyMap=e.anisotropyMap,this.clearcoat=e.clearcoat,this.clearcoatMap=e.clearcoatMap,this.clearcoatRoughness=e.clearcoatRoughness,this.clearcoatRoughnessMap=e.clearcoatRoughnessMap,this.clearcoatNormalMap=e.clearcoatNormalMap,this.clearcoatNormalScale.copy(e.clearcoatNormalScale),this.dispersion=e.dispersion,this.ior=e.ior,this.iridescence=e.iridescence,this.iridescenceMap=e.iridescenceMap,this.iridescenceIOR=e.iridescenceIOR,this.iridescenceThicknessRange=[...e.iridescenceThicknessRange],this.iridescenceThicknessMap=e.iridescenceThicknessMap,this.sheen=e.sheen,this.sheenColor.copy(e.sheenColor),this.sheenColorMap=e.sheenColorMap,this.sheenRoughness=e.sheenRoughness,this.sheenRoughnessMap=e.sheenRoughnessMap,this.transmission=e.transmission,this.transmissionMap=e.transmissionMap,this.thickness=e.thickness,this.thicknessMap=e.thicknessMap,this.attenuationDistance=e.attenuationDistance,this.attenuationColor.copy(e.attenuationColor),this.specularIntensity=e.specularIntensity,this.specularIntensityMap=e.specularIntensityMap,this.specularColor.copy(e.specularColor),this.specularColorMap=e.specularColorMap,this}};var nh=class extends er{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=qS,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},ih=class extends er{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function wf(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var fs=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},rh=class extends fs{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:cy,endingEnd:cy}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case ly:s=e,a=2*t-i;break;case uy:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case ly:o=e,c=2*i-t;break;case uy:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),y=g*g,p=y*g,m=-f*p+2*f*y-f*g,b=(1+f)*p+(-1.5-2*f)*y+(-.5+f)*g+1,M=(-1-h)*p+(1.5+h)*y+.5*g,S=h*p-h*y;for(let A=0;A!==a;++A)s[A]=m*o[u+A]+b*o[l+A]+M*o[c+A]+S*o[d+A];return s}},sh=class extends fs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)s[f]=o[l+f]*d+o[c+f]*u;return s}},oh=class extends fs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},ah=class extends fs{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,d=u.inTangents,f=u.outTangents;if(!d||!f){let y=(i-t)/(r-t),p=1-y;for(let m=0;m!==a;++m)s[m]=o[l+m]*p+o[c+m]*y;return s}let h=a*2,g=e-1;for(let y=0;y!==a;++y){let p=o[l+y],m=o[c+y],b=g*h+y*2,M=f[b],S=f[b+1],A=e*h+y*2,w=d[A],I=d[A+1],_=(i-t)/(r-t),C,O,D,L,G;for(let W=0;W<8;W++){C=_*_,O=C*_,D=1-_,L=D*D,G=L*D;let H=G*t+3*L*_*M+3*D*C*w+O*r-i;if(Math.abs(H)<1e-10)break;let U=3*L*(M-t)+6*D*_*(w-M)+3*C*(r-w);if(Math.abs(U)<1e-10)break;_=_-H/U,_=Math.max(0,Math.min(1,_))}s[y]=G*p+3*L*_*S+3*D*C*I+O*m}return s}},ti=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=wf(t,this.TimeBufferType),this.values=wf(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:wf(e.times,Array),values:wf(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new oh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new sh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new rh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new ah(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case Zc:t=this.InterpolantFactoryMethodDiscrete;break;case zf:t=this.InterpolantFactoryMethodLinear;break;case If:t=this.InterpolantFactoryMethodSmooth;break;case ay:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Re("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Zc;case this.InterpolantFactoryMethodLinear:return zf;case this.InterpolantFactoryMethodSmooth:return If;case this.InterpolantFactoryMethodBezier:return ay}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Pe("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Pe("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Pe("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Pe("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&CR(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Pe("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===If,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,f=d-i,h=d+i;for(let g=0;g!==i;++g){let y=t[d+g];if(y!==t[f+g]||y!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let d=a*i,f=o*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};ti.prototype.ValueTypeName="";ti.prototype.TimeBufferType=Float32Array;ti.prototype.ValueBufferType=Float32Array;ti.prototype.DefaultInterpolation=zf;var hs=class extends ti{constructor(e,t,i){super(e,t,i)}};hs.prototype.ValueTypeName="bool";hs.prototype.ValueBufferType=Array;hs.prototype.DefaultInterpolation=Zc;hs.prototype.InterpolantFactoryMethodLinear=void 0;hs.prototype.InterpolantFactoryMethodSmooth=void 0;var ch=class extends ti{constructor(e,t,i,r){super(e,t,i,r)}};ch.prototype.ValueTypeName="color";var lh=class extends ti{constructor(e,t,i,r){super(e,t,i,r)}};lh.prototype.ValueTypeName="number";var uh=class extends fs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)pn.slerpFlat(s,0,o,l-a,o,l,c);return s}},hl=class extends ti{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new uh(this.times,this.values,this.getValueSize(),e)}};hl.prototype.ValueTypeName="quaternion";hl.prototype.InterpolantFactoryMethodSmooth=void 0;var ps=class extends ti{constructor(e,t,i){super(e,t,i)}};ps.prototype.ValueTypeName="string";ps.prototype.ValueBufferType=Array;ps.prototype.DefaultInterpolation=Zc;ps.prototype.InterpolantFactoryMethodLinear=void 0;ps.prototype.InterpolantFactoryMethodSmooth=void 0;var dh=class extends ti{constructor(e,t,i,r){super(e,t,i,r)}};dh.prototype.ValueTypeName="vector";var pl=class extends Jn{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new Ve(e),this.intensity=t}dispose(){this.dispatchEvent({type:"dispose"})}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}};var iy=new Dt,mS=new T,gS=new T,fy=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.biasNode=null,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new Te(512,512),this.mapType=zn,this.map=null,this.mapPass=null,this.matrix=new Dt,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Ia,this._frameExtents=new Te(1,1),this._viewportCount=1,this._viewports=[new it(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;mS.setFromMatrixPosition(e.matrixWorld),t.position.copy(mS),gS.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(gS),t.updateMatrixWorld(),iy.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(iy,t.coordinateSystem,t.reversedDepth),t.coordinateSystem===Ea||t.reversedDepth?i.set(.5,0,0,.5,0,.5,0,.5,0,0,1,0,0,0,0,1):i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(iy)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.autoUpdate=e.autoUpdate,this.needsUpdate=e.needsUpdate,this.normalBias=e.normalBias,this.blurSamples=e.blurSamples,this.mapSize.copy(e.mapSize),this.biasNode=e.biasNode,this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}},Cf=new T,Tf=new pn,Ki=new T,ml=class extends Jn{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Dt,this.projectionMatrix=new Dt,this.projectionMatrixInverse=new Dt,this.coordinateSystem=Ti,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(Cf,Tf,Ki),Ki.x===1&&Ki.y===1&&Ki.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Cf,Tf,Ki.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(Cf,Tf,Ki),Ki.x===1&&Ki.y===1&&Ki.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Cf,Tf,Ki.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},as=new T,vS=new Te,yS=new Te,Tn=class extends ml{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Ma*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Xc*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Ma*2*Math.atan(Math.tan(Xc*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){as.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(as.x,as.y).multiplyScalar(-e/as.z),as.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(as.x,as.y).multiplyScalar(-e/as.z)}getViewSize(e,t){return this.getViewBounds(e,vS,yS),t.subVectors(yS,vS)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Xc*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var Fa=class extends ml{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},hy=class extends fy{constructor(){super(new Fa(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},La=class extends pl{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(Jn.DEFAULT_UP),this.updateMatrix(),this.target=new Jn,this.shadow=new hy}dispose(){super.dispose(),this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}toJSON(e){let t=super.toJSON(e);return t.object.shadow=this.shadow.toJSON(),t.object.target=this.target.uuid,t}},gl=class extends pl{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var vl=class extends Wt{constructor(){super(),this.isInstancedBufferGeometry=!0,this.type="InstancedBufferGeometry",this.instanceCount=1/0}copy(e){return super.copy(e),this.instanceCount=e.instanceCount,this}toJSON(){let e=super.toJSON();return e.instanceCount=this.instanceCount,e.isInstancedBufferGeometry=!0,e}};var va=-90,ya=1,fh=class extends Jn{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new Tn(va,ya,e,t);r.layers=this.layers,this.add(r);let s=new Tn(va,ya,e,t);s.layers=this.layers,this.add(s);let o=new Tn(va,ya,e,t);o.layers=this.layers,this.add(o);let a=new Tn(va,ya,e,t);a.layers=this.layers,this.add(a);let c=new Tn(va,ya,e,t);c.layers=this.layers,this.add(c);let l=new Tn(va,ya,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===Ti)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Ea)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let y=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let p=!1;e.isWebGLRenderer===!0?p=e.state.buffers.depth.getReversed():p=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),p&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),p&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),p&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),p&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),p&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=y,e.setRenderTarget(i,5,r),p&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},hh=class extends Tn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var By="\\[\\]\\.:\\/",uN=new RegExp("["+By+"]","g"),Vy="[^"+By+"]",dN="[^"+By.replace("\\.","")+"]",fN=/((?:WC+[\/:])*)/.source.replace("WC",Vy),hN=/(WCOD+)?/.source.replace("WCOD",dN),pN=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",Vy),mN=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",Vy),gN=new RegExp("^"+fN+hN+pN+mN+"$"),vN=["material","materials","bones","map"],py=class{constructor(e,t,i){let r=i||jt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},jt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(uN,"")}static parseTrackName(t){let i=gN.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);vN.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Re("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Pe("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Pe("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Pe("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Pe("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Pe("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Pe("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Pe("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Pe("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Pe("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Pe("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=py,n})();jt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};jt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};jt.prototype.GetterByBindingType=[jt.prototype._getValue_direct,jt.prototype._getValue_array,jt.prototype._getValue_arrayElement,jt.prototype._getValue_toArray];jt.prototype.SetterByBindingTypeAndVersioning=[[jt.prototype._setValue_direct,jt.prototype._setValue_direct_setNeedsUpdate,jt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[jt.prototype._setValue_array,jt.prototype._setValue_array_setNeedsUpdate,jt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[jt.prototype._setValue_arrayElement,jt.prototype._setValue_arrayElement_setNeedsUpdate,jt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[jt.prototype._setValue_fromArray,jt.prototype._setValue_fromArray_setNeedsUpdate,jt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var Q6=new Float32Array(1);var ms=class extends sl{constructor(e,t,i=1){super(e,t),this.isInstancedInterleavedBuffer=!0,this.meshPerAttribute=i}copy(e){return super.copy(e),this.meshPerAttribute=e.meshPerAttribute,this}clone(e){let t=super.clone(e);return t.meshPerAttribute=this.meshPerAttribute,t}toJSON(e){let t=super.toJSON(e);return t.isInstancedInterleavedBuffer=!0,t.meshPerAttribute=this.meshPerAttribute,t}};var _S=new Dt,yl=class{constructor(e,t,i=0,r=1/0){this.ray=new ls(e,t),this.near=i,this.far=r,this.camera=null,this.layers=new Ta,this.params={Mesh:{},Line:{threshold:1},LOD:{},Points:{threshold:1},Sprite:{}}}set(e,t){this.ray.set(e,t)}setFromCamera(e,t){t.isPerspectiveCamera?(this.ray.origin.setFromMatrixPosition(t.matrixWorld),this.ray.direction.set(e.x,e.y,.5).unproject(t).sub(this.ray.origin).normalize(),this.camera=t):t.isOrthographicCamera?(this.ray.origin.set(e.x,e.y,(t.near+t.far)/(t.near-t.far)).unproject(t),this.ray.direction.set(0,0,-1).transformDirection(t.matrixWorld),this.camera=t):Pe("Raycaster: Unsupported camera type: "+t.type)}setFromXRController(e){return _S.identity().extractRotation(e.matrixWorld),this.ray.origin.setFromMatrixPosition(e.matrixWorld),this.ray.direction.set(0,0,-1).applyMatrix4(_S),this}intersectObject(e,t=!0,i=[]){return my(e,this,i,t),i.sort(xS),i}intersectObjects(e,t=!0,i=[]){for(let r=0,s=e.length;r<s;r++)my(e[r],this,i,t);return i.sort(xS),i}};function xS(n,e){return n.distance-e.distance}function my(n,e,t,i){let r=!0;if(n.layers.test(e.layers)&&n.raycast(e,t)===!1&&(r=!1),r===!0&&i===!0){let s=n.children;for(let o=0,a=s.length;o<a;o++)my(s[o],e,t,!0)}}var ka=class{constructor(e=1,t=0,i=0){this.radius=e,this.phi=t,this.theta=i}set(e,t,i){return this.radius=e,this.phi=t,this.theta=i,this}copy(e){return this.radius=e.radius,this.phi=e.phi,this.theta=e.theta,this}makeSafe(){return this.phi=Be(this.phi,1e-6,Math.PI-1e-6),this}setFromVector3(e){return this.setFromCartesianCoords(e.x,e.y,e.z)}setFromCartesianCoords(e,t,i){return this.radius=Math.sqrt(e*e+t*t+i*i),this.radius===0?(this.theta=0,this.phi=0):(this.theta=Math.atan2(e,i),this.phi=Math.acos(Be(t/this.radius,-1,1))),this}clone(){return new this.constructor().copy(this)}};var gy=class n{static{n.prototype.isMatrix2=!0}constructor(e,t,i,r){this.elements=[1,0,0,1],e!==void 0&&this.set(e,t,i,r)}identity(){return this.set(1,0,0,1),this}fromArray(e,t=0){for(let i=0;i<4;i++)this.elements[i]=e[i+t];return this}set(e,t,i,r){let s=this.elements;return s[0]=e,s[2]=t,s[1]=i,s[3]=r,this}};var bS=new T,Df=new T,_a=new T,xa=new T,ry=new T,yN=new T,_N=new T,_l=class{constructor(e=new T,t=new T){this.start=e,this.end=t}set(e,t){return this.start.copy(e),this.end.copy(t),this}copy(e){return this.start.copy(e.start),this.end.copy(e.end),this}getCenter(e){return e.addVectors(this.start,this.end).multiplyScalar(.5)}delta(e){return e.subVectors(this.end,this.start)}distanceSq(){return this.start.distanceToSquared(this.end)}distance(){return this.start.distanceTo(this.end)}at(e,t){return this.delta(t).multiplyScalar(e).add(this.start)}closestPointToPointParameter(e,t){bS.subVectors(e,this.start),Df.subVectors(this.end,this.start);let i=Df.dot(Df);if(i===0)return 0;let s=Df.dot(bS)/i;return t&&(s=Be(s,0,1)),s}closestPointToPoint(e,t,i){let r=this.closestPointToPointParameter(e,t);return this.delta(i).multiplyScalar(r).add(this.start)}distanceSqToLine3(e,t=yN,i=_N){let r=10000000000000001e-32,s,o,a=this.start,c=e.start,l=this.end,u=e.end;_a.subVectors(l,a),xa.subVectors(u,c),ry.subVectors(a,c);let d=_a.dot(_a),f=xa.dot(xa),h=xa.dot(ry);if(d<=r&&f<=r)return t.copy(a),i.copy(c),t.sub(i),t.dot(t);if(d<=r)s=0,o=h/f,o=Be(o,0,1);else{let g=_a.dot(ry);if(f<=r)o=0,s=Be(-g/d,0,1);else{let y=_a.dot(xa),p=d*f-y*y;p!==0?s=Be((y*h-g*f)/p,0,1):s=0,o=(y*s+h)/f,o<0?(o=0,s=Be(-g/d,0,1)):o>1&&(o=1,s=Be((y-g)/d,0,1))}}return t.copy(a).addScaledVector(_a,s),i.copy(c).addScaledVector(xa,o),t.distanceToSquared(i)}applyMatrix4(e){return this.start.applyMatrix4(e),this.end.applyMatrix4(e),this}equals(e){return e.start.equals(this.start)&&e.end.equals(this.end)}clone(){return new this.constructor().copy(this)}};var xl=class extends Ra{constructor(e=10,t=10,i=4473924,r=8947848){i=new Ve(i),r=new Ve(r);let s=t/2,o=e/t,a=e/2,c=[],l=[];for(let f=0,h=0,g=-a;f<=t;f++,g+=o){c.push(-a,0,g,a,0,g),c.push(g,0,-a,g,0,a);let y=f===s?i:r;y.toArray(l,h),h+=3,y.toArray(l,h),h+=3,y.toArray(l,h),h+=3,y.toArray(l,h),h+=3}let u=new Wt;u.setAttribute("position",new ut(c,3)),u.setAttribute("color",new ut(l,3));let d=new ds({vertexColors:!0,toneMapped:!1});super(u,d),this.type="GridHelper"}dispose(){this.geometry.dispose(),this.material.dispose()}};var ES=new T,Af,sy,bl=class extends Jn{constructor(e=new T(0,0,1),t=new T(0,0,0),i=1,r=16776960,s=i*.2,o=s*.2){super(),this.type="ArrowHelper",Af===void 0&&(Af=new Wt,Af.setAttribute("position",new ut([0,0,0,0,1,0],3)),sy=new Qf(.5,1,5,1),sy.translate(0,-.5,0)),this.position.copy(t),this.line=new al(Af,new ds({color:r,toneMapped:!1})),this.line.matrixAutoUpdate=!1,this.add(this.line),this.cone=new tn(sy,new us({color:r,toneMapped:!1})),this.cone.matrixAutoUpdate=!1,this.add(this.cone),this.setDirection(e),this.setLength(i,s,o)}setDirection(e){if(e.y>.99999)this.quaternion.set(0,0,0,1);else if(e.y<-.99999)this.quaternion.set(1,0,0,0);else{ES.set(e.z,0,-e.x).normalize();let t=Math.acos(e.y);this.quaternion.setFromAxisAngle(ES,t)}}setLength(e,t=e*.2,i=t*.2){this.line.scale.set(1,Math.max(1e-4,e-t),1),this.line.updateMatrix(),this.cone.scale.set(i,t,i),this.cone.position.y=e,this.cone.updateMatrix()}setColor(e){this.line.material.color.set(e),this.cone.material.color.set(e)}copy(e){return super.copy(e,!1),this.line.copy(e.line),this.cone.copy(e.cone),this}dispose(){this.line.geometry.dispose(),this.line.material.dispose(),this.cone.geometry.dispose(),this.cone.material.dispose()}};var El=class extends Di{constructor(e,t=null){super(),this.object=e,this.domElement=t,this.enabled=!0,this.state=-1,this.keys={},this.mouseButtons={LEFT:null,MIDDLE:null,RIGHT:null},this.touches={ONE:null,TWO:null}}connect(e){if(e===void 0){Re("Controls: connect() now requires an element.");return}this.domElement!==null&&this.disconnect(),this.domElement=e}disconnect(){}dispose(){}update(){}};function Hy(n,e,t,i){let r=xN(i);switch(t){case Py:return n*e;case Fy:return n*e/r.components*r.byteLength;case xh:return n*e/r.components*r.byteLength;case bs:return n*e*2/r.components*r.byteLength;case bh:return n*e*2/r.components*r.byteLength;case Oy:return n*e*3/r.components*r.byteLength;case hi:return n*e*4/r.components*r.byteLength;case Eh:return n*e*4/r.components*r.byteLength;case Cl:case Tl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Dl:case Al:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Mh:case Ch:return Math.max(n,16)*Math.max(e,8)/4;case Sh:case wh:return Math.max(n,8)*Math.max(e,8)/2;case Th:case Dh:case Ih:case Rh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Ah:case Il:case Nh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Ph:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Oh:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case Fh:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Lh:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case kh:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Uh:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Bh:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case Vh:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Hh:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case zh:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Gh:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case jh:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Wh:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case $h:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case qh:case Xh:case Yh:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Zh:case Kh:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Rl:case Jh:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function xN(n){switch(n){case zn:case Ay:return{byteLength:1,components:1};case Ba:case Iy:case nr:return{byteLength:2,components:1};case yh:case _h:return{byteLength:2,components:4};case Ii:case vh:case Ri:return{byteLength:4,components:1};case Ry:case Ny:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"184"}}));typeof window<"u"&&(window.__THREE__?Re("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="184");function AM(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&n!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n!==null&&n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function EN(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,f=n.createBuffer();n.bindBuffer(c,f),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:f,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((h,g)=>h.start-g.start);let f=0;for(let h=1;h<d.length;h++){let g=d[f],y=d[h];y.start<=g.start+g.count+1?g.count=Math.max(g.count,y.start+y.count-g.start):(++f,d[f]=y)}d.length=f+1;for(let h=0,g=d.length;h<g;h++){let y=d[h];n.bufferSubData(l,y.start*u.BYTES_PER_ELEMENT,u,y.start,y.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var SN=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,MN=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,wN=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,CN=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,TN=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,DN=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,AN=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,IN=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,RN=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,NN=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,PN=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,ON=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,FN=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,LN=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,kN=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,UN=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,BN=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,VN=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,HN=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,zN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,GN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,jN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,WN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,$N=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,qN=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,XN=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,YN=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,ZN=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,KN=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,JN=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,QN="gl_FragColor = linearToOutputTexel( gl_FragColor );",eP=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,tP=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * reflectVec );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,nP=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,iP=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,rP=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,sP=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,oP=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,aP=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,cP=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,lP=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,uP=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,dP=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,fP=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,hP=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,pP=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif
#include <lightprobes_pars_fragment>`,mP=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,gP=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,vP=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,yP=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,_P=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,xP=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,bP=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		return 0.5 / max( gv + gl, EPSILON );
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,EP=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
	#ifdef USE_LIGHT_PROBES_GRID
		vec3 probeWorldPos = ( ( vec4( geometryPosition, 1.0 ) - viewMatrix[ 3 ] ) * viewMatrix ).xyz;
		vec3 probeWorldNormal = inverseTransformDirection( geometryNormal, viewMatrix );
		irradiance += getLightProbeGridIrradiance( probeWorldPos, probeWorldNormal );
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,SP=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,MP=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,wP=`#ifdef USE_LIGHT_PROBES_GRID
uniform highp sampler3D probesSH;
uniform vec3 probesMin;
uniform vec3 probesMax;
uniform vec3 probesResolution;
vec3 getLightProbeGridIrradiance( vec3 worldPos, vec3 worldNormal ) {
	vec3 res = probesResolution;
	vec3 gridRange = probesMax - probesMin;
	vec3 resMinusOne = res - 1.0;
	vec3 probeSpacing = gridRange / resMinusOne;
	vec3 samplePos = worldPos + worldNormal * probeSpacing * 0.5;
	vec3 uvw = clamp( ( samplePos - probesMin ) / gridRange, 0.0, 1.0 );
	uvw = uvw * resMinusOne / res + 0.5 / res;
	float nz          = res.z;
	float paddedSlices = nz + 2.0;
	float atlasDepth  = 7.0 * paddedSlices;
	float uvZBase     = uvw.z * nz + 1.0;
	vec4 s0 = texture( probesSH, vec3( uvw.xy, ( uvZBase                       ) / atlasDepth ) );
	vec4 s1 = texture( probesSH, vec3( uvw.xy, ( uvZBase +       paddedSlices   ) / atlasDepth ) );
	vec4 s2 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 2.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s3 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 3.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s4 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 4.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s5 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 5.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s6 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 6.0 * paddedSlices   ) / atlasDepth ) );
	vec3 c0 = s0.xyz;
	vec3 c1 = vec3( s0.w, s1.xy );
	vec3 c2 = vec3( s1.zw, s2.x );
	vec3 c3 = s2.yzw;
	vec3 c4 = s3.xyz;
	vec3 c5 = vec3( s3.w, s4.xy );
	vec3 c6 = vec3( s4.zw, s5.x );
	vec3 c7 = s5.yzw;
	vec3 c8 = s6.xyz;
	float x = worldNormal.x, y = worldNormal.y, z = worldNormal.z;
	vec3 result = c0 * 0.886227;
	result += c1 * 2.0 * 0.511664 * y;
	result += c2 * 2.0 * 0.511664 * z;
	result += c3 * 2.0 * 0.511664 * x;
	result += c4 * 2.0 * 0.429043 * x * y;
	result += c5 * 2.0 * 0.429043 * y * z;
	result += c6 * ( 0.743125 * z * z - 0.247708 );
	result += c7 * 2.0 * 0.429043 * x * z;
	result += c8 * 0.429043 * ( x * x - y * y );
	return max( result, vec3( 0.0 ) );
}
#endif`,CP=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,TP=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,DP=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,AP=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,IP=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,RP=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,NP=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,PP=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,OP=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,FP=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,LP=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,kP=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,UP=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,BP=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,VP=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,HP=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,zP=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#if defined( USE_PACKED_NORMALMAP )
		mapN = vec3( mapN.xy, sqrt( saturate( 1.0 - dot( mapN.xy, mapN.xy ) ) ) );
	#endif
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,GP=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,jP=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,WP=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,$P=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,qP=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,XP=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,YP=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,ZP=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,KP=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,JP=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,QP=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,eO=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,tO=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,nO=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,iO=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,rO=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,sO=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,oO=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,aO=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	#ifdef HAS_NORMAL
		vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	#else
		vec3 shadowWorldNormal = vec3( 0.0 );
	#endif
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,cO=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,lO=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,uO=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,dO=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,fO=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,hO=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,pO=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,mO=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,gO=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,vO=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,yO=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,_O=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,xO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,bO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,EO=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,SO=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,MO=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,wO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,CO=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vWorldDirection );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,TO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,DO=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,AO=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,IO=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,RO=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,NO=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,PO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,OO=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,FO=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,LO=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,kO=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,UO=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,BO=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,VO=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,HO=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,zO=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,GO=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,jO=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,WO=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,$O=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,qO=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,XO=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,YO=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,ZO=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,KO=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,JO=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,QO=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,eF=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,tF=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,nF=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,Je={alphahash_fragment:SN,alphahash_pars_fragment:MN,alphamap_fragment:wN,alphamap_pars_fragment:CN,alphatest_fragment:TN,alphatest_pars_fragment:DN,aomap_fragment:AN,aomap_pars_fragment:IN,batching_pars_vertex:RN,batching_vertex:NN,begin_vertex:PN,beginnormal_vertex:ON,bsdfs:FN,iridescence_fragment:LN,bumpmap_pars_fragment:kN,clipping_planes_fragment:UN,clipping_planes_pars_fragment:BN,clipping_planes_pars_vertex:VN,clipping_planes_vertex:HN,color_fragment:zN,color_pars_fragment:GN,color_pars_vertex:jN,color_vertex:WN,common:$N,cube_uv_reflection_fragment:qN,defaultnormal_vertex:XN,displacementmap_pars_vertex:YN,displacementmap_vertex:ZN,emissivemap_fragment:KN,emissivemap_pars_fragment:JN,colorspace_fragment:QN,colorspace_pars_fragment:eP,envmap_fragment:tP,envmap_common_pars_fragment:nP,envmap_pars_fragment:iP,envmap_pars_vertex:rP,envmap_physical_pars_fragment:mP,envmap_vertex:sP,fog_vertex:oP,fog_pars_vertex:aP,fog_fragment:cP,fog_pars_fragment:lP,gradientmap_pars_fragment:uP,lightmap_pars_fragment:dP,lights_lambert_fragment:fP,lights_lambert_pars_fragment:hP,lights_pars_begin:pP,lights_toon_fragment:gP,lights_toon_pars_fragment:vP,lights_phong_fragment:yP,lights_phong_pars_fragment:_P,lights_physical_fragment:xP,lights_physical_pars_fragment:bP,lights_fragment_begin:EP,lights_fragment_maps:SP,lights_fragment_end:MP,lightprobes_pars_fragment:wP,logdepthbuf_fragment:CP,logdepthbuf_pars_fragment:TP,logdepthbuf_pars_vertex:DP,logdepthbuf_vertex:AP,map_fragment:IP,map_pars_fragment:RP,map_particle_fragment:NP,map_particle_pars_fragment:PP,metalnessmap_fragment:OP,metalnessmap_pars_fragment:FP,morphinstance_vertex:LP,morphcolor_vertex:kP,morphnormal_vertex:UP,morphtarget_pars_vertex:BP,morphtarget_vertex:VP,normal_fragment_begin:HP,normal_fragment_maps:zP,normal_pars_fragment:GP,normal_pars_vertex:jP,normal_vertex:WP,normalmap_pars_fragment:$P,clearcoat_normal_fragment_begin:qP,clearcoat_normal_fragment_maps:XP,clearcoat_pars_fragment:YP,iridescence_pars_fragment:ZP,opaque_fragment:KP,packing:JP,premultiplied_alpha_fragment:QP,project_vertex:eO,dithering_fragment:tO,dithering_pars_fragment:nO,roughnessmap_fragment:iO,roughnessmap_pars_fragment:rO,shadowmap_pars_fragment:sO,shadowmap_pars_vertex:oO,shadowmap_vertex:aO,shadowmask_pars_fragment:cO,skinbase_vertex:lO,skinning_pars_vertex:uO,skinning_vertex:dO,skinnormal_vertex:fO,specularmap_fragment:hO,specularmap_pars_fragment:pO,tonemapping_fragment:mO,tonemapping_pars_fragment:gO,transmission_fragment:vO,transmission_pars_fragment:yO,uv_pars_fragment:_O,uv_pars_vertex:xO,uv_vertex:bO,worldpos_vertex:EO,background_vert:SO,background_frag:MO,backgroundCube_vert:wO,backgroundCube_frag:CO,cube_vert:TO,cube_frag:DO,depth_vert:AO,depth_frag:IO,distance_vert:RO,distance_frag:NO,equirect_vert:PO,equirect_frag:OO,linedashed_vert:FO,linedashed_frag:LO,meshbasic_vert:kO,meshbasic_frag:UO,meshlambert_vert:BO,meshlambert_frag:VO,meshmatcap_vert:HO,meshmatcap_frag:zO,meshnormal_vert:GO,meshnormal_frag:jO,meshphong_vert:WO,meshphong_frag:$O,meshphysical_vert:qO,meshphysical_frag:XO,meshtoon_vert:YO,meshtoon_frag:ZO,points_vert:KO,points_frag:JO,shadow_vert:QO,shadow_frag:eF,sprite_vert:tF,sprite_frag:nF},ae={common:{diffuse:{value:new Ve(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new je},alphaMap:{value:null},alphaMapTransform:{value:new je},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new je}},envmap:{envMap:{value:null},envMapRotation:{value:new je},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new je}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new je}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new je},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new je},normalScale:{value:new Te(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new je},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new je}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new je}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new je}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new Ve(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null},probesSH:{value:null},probesMin:{value:new T},probesMax:{value:new T},probesResolution:{value:new T}},points:{diffuse:{value:new Ve(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new je},alphaTest:{value:0},uvTransform:{value:new je}},sprite:{diffuse:{value:new Ve(16777215)},opacity:{value:1},center:{value:new Te(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new je},alphaMap:{value:null},alphaMapTransform:{value:new je},alphaTest:{value:0}}},On={basic:{uniforms:An([ae.common,ae.specularmap,ae.envmap,ae.aomap,ae.lightmap,ae.fog]),vertexShader:Je.meshbasic_vert,fragmentShader:Je.meshbasic_frag},lambert:{uniforms:An([ae.common,ae.specularmap,ae.envmap,ae.aomap,ae.lightmap,ae.emissivemap,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.fog,ae.lights,{emissive:{value:new Ve(0)},envMapIntensity:{value:1}}]),vertexShader:Je.meshlambert_vert,fragmentShader:Je.meshlambert_frag},phong:{uniforms:An([ae.common,ae.specularmap,ae.envmap,ae.aomap,ae.lightmap,ae.emissivemap,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.fog,ae.lights,{emissive:{value:new Ve(0)},specular:{value:new Ve(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:Je.meshphong_vert,fragmentShader:Je.meshphong_frag},standard:{uniforms:An([ae.common,ae.envmap,ae.aomap,ae.lightmap,ae.emissivemap,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.roughnessmap,ae.metalnessmap,ae.fog,ae.lights,{emissive:{value:new Ve(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Je.meshphysical_vert,fragmentShader:Je.meshphysical_frag},toon:{uniforms:An([ae.common,ae.aomap,ae.lightmap,ae.emissivemap,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.gradientmap,ae.fog,ae.lights,{emissive:{value:new Ve(0)}}]),vertexShader:Je.meshtoon_vert,fragmentShader:Je.meshtoon_frag},matcap:{uniforms:An([ae.common,ae.bumpmap,ae.normalmap,ae.displacementmap,ae.fog,{matcap:{value:null}}]),vertexShader:Je.meshmatcap_vert,fragmentShader:Je.meshmatcap_frag},points:{uniforms:An([ae.points,ae.fog]),vertexShader:Je.points_vert,fragmentShader:Je.points_frag},dashed:{uniforms:An([ae.common,ae.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Je.linedashed_vert,fragmentShader:Je.linedashed_frag},depth:{uniforms:An([ae.common,ae.displacementmap]),vertexShader:Je.depth_vert,fragmentShader:Je.depth_frag},normal:{uniforms:An([ae.common,ae.bumpmap,ae.normalmap,ae.displacementmap,{opacity:{value:1}}]),vertexShader:Je.meshnormal_vert,fragmentShader:Je.meshnormal_frag},sprite:{uniforms:An([ae.sprite,ae.fog]),vertexShader:Je.sprite_vert,fragmentShader:Je.sprite_frag},background:{uniforms:{uvTransform:{value:new je},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Je.background_vert,fragmentShader:Je.background_frag},backgroundCube:{uniforms:{envMap:{value:null},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new je}},vertexShader:Je.backgroundCube_vert,fragmentShader:Je.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Je.cube_vert,fragmentShader:Je.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Je.equirect_vert,fragmentShader:Je.equirect_frag},distance:{uniforms:An([ae.common,ae.displacementmap,{referencePosition:{value:new T},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Je.distance_vert,fragmentShader:Je.distance_frag},shadow:{uniforms:An([ae.lights,ae.fog,{color:{value:new Ve(0)},opacity:{value:1}}]),vertexShader:Je.shadow_vert,fragmentShader:Je.shadow_frag}};On.physical={uniforms:An([On.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new je},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new je},clearcoatNormalScale:{value:new Te(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new je},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new je},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new je},sheen:{value:0},sheenColor:{value:new Ve(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new je},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new je},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new je},transmissionSamplerSize:{value:new Te},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new je},attenuationDistance:{value:0},attenuationColor:{value:new Ve(0)},specularColor:{value:new Ve(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new je},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new je},anisotropyVector:{value:new Te},anisotropyMap:{value:null},anisotropyMapTransform:{value:new je}}]),vertexShader:Je.meshphysical_vert,fragmentShader:Je.meshphysical_frag};var np={r:0,b:0,g:0},iF=new Dt,IM=new je;IM.set(-1,0,0,0,1,0,0,0,1);function rF(n,e,t,i,r,s){let o=new Ve(0),a=r===!0?0:1,c,l,u=null,d=0,f=null;function h(b){let M=b.isScene===!0?b.background:null;if(M&&M.isTexture){let S=b.backgroundBlurriness>0;M=e.get(M,S)}return M}function g(b){let M=!1,S=h(b);S===null?p(o,a):S&&S.isColor&&(p(S,1),M=!0);let A=n.xr.getEnvironmentBlendMode();A==="additive"?t.buffers.color.setClear(0,0,0,1,s):A==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||M)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function y(b,M){let S=h(M);S&&(S.isCubeTexture||S.mapping===Ml)?(l===void 0&&(l=new tn(new Na(1,1,1),new Nn({name:"BackgroundCubeMaterial",uniforms:co(On.backgroundCube.uniforms),vertexShader:On.backgroundCube.vertexShader,fragmentShader:On.backgroundCube.fragmentShader,side:Pn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(A,w,I){this.matrixWorld.copyPosition(I.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),l.material.uniforms.envMap.value=S,l.material.uniforms.backgroundBlurriness.value=M.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=M.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(iF.makeRotationFromEuler(M.backgroundRotation)).transpose(),S.isCubeTexture&&S.isRenderTargetTexture===!1&&l.material.uniforms.backgroundRotation.value.premultiply(IM),l.material.toneMapped=at.getTransfer(S.colorSpace)!==pt,(u!==S||d!==S.version||f!==n.toneMapping)&&(l.material.needsUpdate=!0,u=S,d=S.version,f=n.toneMapping),l.layers.enableAll(),b.unshift(l,l.geometry,l.material,0,0,null)):S&&S.isTexture&&(c===void 0&&(c=new tn(new dl(2,2),new Nn({name:"BackgroundMaterial",uniforms:co(On.background.uniforms),vertexShader:On.background.vertexShader,fragmentShader:On.background.fragmentShader,side:Rr,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=S,c.material.uniforms.backgroundIntensity.value=M.backgroundIntensity,c.material.toneMapped=at.getTransfer(S.colorSpace)!==pt,S.matrixAutoUpdate===!0&&S.updateMatrix(),c.material.uniforms.uvTransform.value.copy(S.matrix),(u!==S||d!==S.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,u=S,d=S.version,f=n.toneMapping),c.layers.enableAll(),b.unshift(c,c.geometry,c.material,0,0,null))}function p(b,M){b.getRGB(np,Uy(n)),t.buffers.color.setClear(np.r,np.g,np.b,M,s)}function m(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(b,M=1){o.set(b),a=M,p(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(b){a=b,p(o,a)},render:g,addToRenderList:y,dispose:m}}function sF(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),s=r,o=!1;function a(D,L,G,W,P){let H=!1,U=d(D,W,G,L);s!==U&&(s=U,l(s.object)),H=h(D,W,G,P),H&&g(D,W,G,P),P!==null&&e.update(P,n.ELEMENT_ARRAY_BUFFER),(H||o)&&(o=!1,S(D,L,G,W),P!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(P).buffer))}function c(){return n.createVertexArray()}function l(D){return n.bindVertexArray(D)}function u(D){return n.deleteVertexArray(D)}function d(D,L,G,W){let P=W.wireframe===!0,H=i[L.id];H===void 0&&(H={},i[L.id]=H);let U=D.isInstancedMesh===!0?D.id:0,J=H[U];J===void 0&&(J={},H[U]=J);let Q=J[G.id];Q===void 0&&(Q={},J[G.id]=Q);let ce=Q[P];return ce===void 0&&(ce=f(c()),Q[P]=ce),ce}function f(D){let L=[],G=[],W=[];for(let P=0;P<t;P++)L[P]=0,G[P]=0,W[P]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:L,enabledAttributes:G,attributeDivisors:W,object:D,attributes:{},index:null}}function h(D,L,G,W){let P=s.attributes,H=L.attributes,U=0,J=G.getAttributes();for(let Q in J)if(J[Q].location>=0){let xe=P[Q],Se=H[Q];if(Se===void 0&&(Q==="instanceMatrix"&&D.instanceMatrix&&(Se=D.instanceMatrix),Q==="instanceColor"&&D.instanceColor&&(Se=D.instanceColor)),xe===void 0||xe.attribute!==Se||Se&&xe.data!==Se.data)return!0;U++}return s.attributesNum!==U||s.index!==W}function g(D,L,G,W){let P={},H=L.attributes,U=0,J=G.getAttributes();for(let Q in J)if(J[Q].location>=0){let xe=H[Q];xe===void 0&&(Q==="instanceMatrix"&&D.instanceMatrix&&(xe=D.instanceMatrix),Q==="instanceColor"&&D.instanceColor&&(xe=D.instanceColor));let Se={};Se.attribute=xe,xe&&xe.data&&(Se.data=xe.data),P[Q]=Se,U++}s.attributes=P,s.attributesNum=U,s.index=W}function y(){let D=s.newAttributes;for(let L=0,G=D.length;L<G;L++)D[L]=0}function p(D){m(D,0)}function m(D,L){let G=s.newAttributes,W=s.enabledAttributes,P=s.attributeDivisors;G[D]=1,W[D]===0&&(n.enableVertexAttribArray(D),W[D]=1),P[D]!==L&&(n.vertexAttribDivisor(D,L),P[D]=L)}function b(){let D=s.newAttributes,L=s.enabledAttributes;for(let G=0,W=L.length;G<W;G++)L[G]!==D[G]&&(n.disableVertexAttribArray(G),L[G]=0)}function M(D,L,G,W,P,H,U){U===!0?n.vertexAttribIPointer(D,L,G,P,H):n.vertexAttribPointer(D,L,G,W,P,H)}function S(D,L,G,W){y();let P=W.attributes,H=G.getAttributes(),U=L.defaultAttributeValues;for(let J in H){let Q=H[J];if(Q.location>=0){let ce=P[J];if(ce===void 0&&(J==="instanceMatrix"&&D.instanceMatrix&&(ce=D.instanceMatrix),J==="instanceColor"&&D.instanceColor&&(ce=D.instanceColor)),ce!==void 0){let xe=ce.normalized,Se=ce.itemSize,rt=e.get(ce);if(rt===void 0)continue;let Ke=rt.buffer,Fe=rt.type,X=rt.bytesPerElement,le=Fe===n.INT||Fe===n.UNSIGNED_INT||ce.gpuType===vh;if(ce.isInterleavedBufferAttribute){let ee=ce.data,Oe=ee.stride,We=ce.offset;if(ee.isInstancedInterleavedBuffer){for(let Le=0;Le<Q.locationSize;Le++)m(Q.location+Le,ee.meshPerAttribute);D.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=ee.meshPerAttribute*ee.count)}else for(let Le=0;Le<Q.locationSize;Le++)p(Q.location+Le);n.bindBuffer(n.ARRAY_BUFFER,Ke);for(let Le=0;Le<Q.locationSize;Le++)M(Q.location+Le,Se/Q.locationSize,Fe,xe,Oe*X,(We+Se/Q.locationSize*Le)*X,le)}else{if(ce.isInstancedBufferAttribute){for(let ee=0;ee<Q.locationSize;ee++)m(Q.location+ee,ce.meshPerAttribute);D.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=ce.meshPerAttribute*ce.count)}else for(let ee=0;ee<Q.locationSize;ee++)p(Q.location+ee);n.bindBuffer(n.ARRAY_BUFFER,Ke);for(let ee=0;ee<Q.locationSize;ee++)M(Q.location+ee,Se/Q.locationSize,Fe,xe,Se*X,Se/Q.locationSize*ee*X,le)}}else if(U!==void 0){let xe=U[J];if(xe!==void 0)switch(xe.length){case 2:n.vertexAttrib2fv(Q.location,xe);break;case 3:n.vertexAttrib3fv(Q.location,xe);break;case 4:n.vertexAttrib4fv(Q.location,xe);break;default:n.vertexAttrib1fv(Q.location,xe)}}}}b()}function A(){C();for(let D in i){let L=i[D];for(let G in L){let W=L[G];for(let P in W){let H=W[P];for(let U in H)u(H[U].object),delete H[U];delete W[P]}}delete i[D]}}function w(D){if(i[D.id]===void 0)return;let L=i[D.id];for(let G in L){let W=L[G];for(let P in W){let H=W[P];for(let U in H)u(H[U].object),delete H[U];delete W[P]}}delete i[D.id]}function I(D){for(let L in i){let G=i[L];for(let W in G){let P=G[W];if(P[D.id]===void 0)continue;let H=P[D.id];for(let U in H)u(H[U].object),delete H[U];delete P[D.id]}}}function _(D){for(let L in i){let G=i[L],W=D.isInstancedMesh===!0?D.id:0,P=G[W];if(P!==void 0){for(let H in P){let U=P[H];for(let J in U)u(U[J].object),delete U[J];delete P[H]}delete G[W],Object.keys(G).length===0&&delete i[L]}}}function C(){O(),o=!0,s!==r&&(s=r,l(s.object))}function O(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:C,resetDefaultState:O,dispose:A,releaseStatesOfGeometry:w,releaseStatesOfObject:_,releaseStatesOfProgram:I,initAttributes:y,enableAttribute:p,disableUnusedAttributes:b}}function oF(n,e,t){let i;function r(c){i=c}function s(c,l){n.drawArrays(i,c,l),t.update(l,i,1)}function o(c,l,u){u!==0&&(n.drawArraysInstanced(i,c,l,u),t.update(l,i,u))}function a(c,l,u){if(u===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,l,0,u);let f=0;for(let h=0;h<u;h++)f+=l[h];t.update(f,i,1)}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a}function aF(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let I=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(I.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(I){return!(I!==hi&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(I){let _=I===nr&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(I!==zn&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&I!==Ri&&!_)}function c(I){if(I==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";I="mediump"}return I==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Re("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,f=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control");t.reversedDepthBuffer===!0&&f===!1&&Re("WebGLRenderer: Unable to use reversed depth buffer due to missing EXT_clip_control extension. Fallback to default depth buffer.");let h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),y=n.getParameter(n.MAX_TEXTURE_SIZE),p=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),m=n.getParameter(n.MAX_VERTEX_ATTRIBS),b=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),M=n.getParameter(n.MAX_VARYING_VECTORS),S=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),A=n.getParameter(n.MAX_SAMPLES),w=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reversedDepthBuffer:f,maxTextures:h,maxVertexTextures:g,maxTextureSize:y,maxCubemapSize:p,maxAttributes:m,maxVertexUniforms:b,maxVaryings:M,maxFragmentUniforms:S,maxSamples:A,samples:w}}function cF(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Vn,a=new je,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let g=d.clippingPlanes,y=d.clipIntersection,p=d.clipShadows,m=n.get(d);if(!r||g===null||g.length===0||s&&!p)s?u(null):l();else{let b=s?0:i,M=b*4,S=m.clippingState||null;c.value=S,S=u(g,f,M,h);for(let A=0;A!==M;++A)S[A]=t[A];m.clippingState=S,this.numIntersection=y?this.numPlanes:0,this.numPlanes+=b}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,g){let y=d!==null?d.length:0,p=null;if(y!==0){if(p=c.value,g!==!0||p===null){let m=h+y*4,b=f.matrixWorldInverse;a.getNormalMatrix(b),(p===null||p.length<m)&&(p=new Float32Array(m));for(let M=0,S=h;M!==y;++M,S+=4)o.copy(d[M]).applyMatrix4(b,a),o.normal.toArray(p,S),p[S+3]=o.constant}c.value=p,c.needsUpdate=!0}return e.numPlanes=y,e.numIntersection=0,p}}var Ss=4,aM=[.125,.215,.35,.446,.526,.582],lo=20,lF=256,Pl=new Fa,cM=new Ve,zy=null,Gy=0,jy=0,Wy=!1,uF=new T,rp=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=uF}=s;zy=this._renderer.getRenderTarget(),Gy=this._renderer.getActiveCubeFace(),jy=this._renderer.getActiveMipmapLevel(),Wy=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=dM(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=uM(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(zy,Gy,jy),this._renderer.xr.enabled=Wy,e.scissorTest=!1,Ha(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===ys||e.mapping===ao?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),zy=this._renderer.getRenderTarget(),Gy=this._renderer.getActiveCubeFace(),jy=this._renderer.getActiveMipmapLevel(),Wy=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:hn,minFilter:hn,generateMipmaps:!1,type:nr,format:hi,colorSpace:Kc,depthBuffer:!1},r=lM(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=lM(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=dF(s)),this._blurMaterial=hF(s,e,t),this._ggxMaterial=fF(s,e,t)}return r}_compileMaterial(e){let t=new tn(new Wt,e);this._renderer.compile(t,Pl)}_sceneToCubeUV(e,t,i,r,s){let c=new Tn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],d=this._renderer,f=d.autoClear,h=d.toneMapping;d.getClearColor(cM),d.toneMapping=Ai,d.autoClear=!1,d.state.buffers.depth.getReversed()&&(d.setRenderTarget(r),d.clearDepth(),d.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new tn(new Na,new us({name:"PMREM.Background",side:Pn,depthWrite:!1,depthTest:!1})));let y=this._backgroundBox,p=y.material,m=!1,b=e.background;b?b.isColor&&(p.color.copy(b),e.background=null,m=!0):(p.color.copy(cM),m=!0);for(let M=0;M<6;M++){let S=M%3;S===0?(c.up.set(0,l[M],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[M],s.y,s.z)):S===1?(c.up.set(0,0,l[M]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[M],s.z)):(c.up.set(0,l[M],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[M]));let A=this._cubeSize;Ha(r,S*A,M>2?A:0,A,A),d.setRenderTarget(r),m&&d.render(y,c),d.render(e,c)}d.toneMapping=h,d.autoClear=f,e.background=b}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===ys||e.mapping===ao;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=dM()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=uM());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Ha(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,Pl)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),d=Math.sqrt(l*l-u*u),f=0+l*1.25,h=d*f,{_lodMax:g}=this,y=this._sizeLods[i],p=3*y*(i>g-Ss?i-g+Ss:0),m=4*(this._cubeSize-y);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=g-t,Ha(s,p,m,3*y,2*y),r.setRenderTarget(s),r.render(a,Pl),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=g-i,Ha(e,p,m,3*y,2*y),r.setRenderTarget(e),r.render(a,Pl)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&Pe("blur direction must be either latitudinal or longitudinal!");let u=3,d=this._lodMeshes[r];d.material=l;let f=l.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*lo-1),y=s/g,p=isFinite(s)?1+Math.floor(u*y):lo;p>lo&&Re(`sigmaRadians, ${s}, is too large and will clip, as it requested ${p} samples when the maximum is set to ${lo}`);let m=[],b=0;for(let I=0;I<lo;++I){let _=I/y,C=Math.exp(-_*_/2);m.push(C),I===0?b+=C:I<p&&(b+=2*C)}for(let I=0;I<m.length;I++)m[I]=m[I]/b;f.envMap.value=e.texture,f.samples.value=p,f.weights.value=m,f.latitudinal.value=o==="latitudinal",a&&(f.poleAxis.value=a);let{_lodMax:M}=this;f.dTheta.value=g,f.mipInt.value=M-i;let S=this._sizeLods[r],A=3*S*(r>M-Ss?r-M+Ss:0),w=4*(this._cubeSize-S);Ha(t,A,w,3*S,2*S),c.setRenderTarget(t),c.render(d,Pl)}};function dF(n){let e=[],t=[],i=[],r=n,s=n-Ss+1+aM.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-Ss?c=aM[o-n+Ss-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,d=1+l,f=[u,u,d,u,d,d,u,u,d,d,u,d],h=6,g=6,y=3,p=2,m=1,b=new Float32Array(y*g*h),M=new Float32Array(p*g*h),S=new Float32Array(m*g*h);for(let w=0;w<h;w++){let I=w%3*2/3-1,_=w>2?0:-1,C=[I,_,0,I+2/3,_,0,I+2/3,_+1,0,I,_,0,I+2/3,_+1,0,I,_+1,0];b.set(C,y*g*w),M.set(f,p*g*w);let O=[w,w,w,w,w,w];S.set(O,m*g*w)}let A=new Wt;A.setAttribute("position",new Hn(b,y)),A.setAttribute("uv",new Hn(M,p)),A.setAttribute("faceIndex",new Hn(S,m)),i.push(new tn(A,null)),r>Ss&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function lM(n,e,t){let i=new Kn(n,e,t);return i.texture.mapping=Ml,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Ha(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function fF(n,e,t){return new Nn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:lF,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:ap(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:tr,depthTest:!1,depthWrite:!1})}function hF(n,e,t){let i=new Float32Array(lo),r=new T(0,1,0);return new Nn({name:"SphericalGaussianBlur",defines:{n:lo,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:ap(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:tr,depthTest:!1,depthWrite:!1})}function uM(){return new Nn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:ap(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:tr,depthTest:!1,depthWrite:!1})}function dM(){return new Nn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:ap(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:tr,depthTest:!1,depthWrite:!1})}function ap(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var sp=class extends Kn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new cl(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new Na(5,5,5),s=new Nn({name:"CubemapFromEquirect",uniforms:co(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:Pn,blending:tr});s.uniforms.tEquirect.value=t;let o=new tn(r,s),a=t.minFilter;return t.minFilter===_s&&(t.minFilter=hn),new fh(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function pF(n){let e=new WeakMap,t=new WeakMap,i=null;function r(f,h=!1){return f==null?null:h?o(f):s(f)}function s(f){if(f&&f.isTexture){let h=f.mapping;if(h===ph||h===mh)if(e.has(f)){let g=e.get(f).texture;return a(g,f.mapping)}else{let g=f.image;if(g&&g.height>0){let y=new sp(g.height);return y.fromEquirectangularTexture(n,f),e.set(f,y),f.addEventListener("dispose",l),a(y.texture,f.mapping)}else return null}}return f}function o(f){if(f&&f.isTexture){let h=f.mapping,g=h===ph||h===mh,y=h===ys||h===ao;if(g||y){let p=t.get(f),m=p!==void 0?p.texture.pmremVersion:0;if(f.isRenderTargetTexture&&f.pmremVersion!==m)return i===null&&(i=new rp(n)),p=g?i.fromEquirectangular(f,p):i.fromCubemap(f,p),p.texture.pmremVersion=f.pmremVersion,t.set(f,p),p.texture;if(p!==void 0)return p.texture;{let b=f.image;return g&&b&&b.height>0||y&&b&&c(b)?(i===null&&(i=new rp(n)),p=g?i.fromEquirectangular(f):i.fromCubemap(f),p.texture.pmremVersion=f.pmremVersion,t.set(f,p),f.addEventListener("dispose",u),p.texture):null}}}return f}function a(f,h){return h===ph?f.mapping=ys:h===mh&&(f.mapping=ao),f}function c(f){let h=0,g=6;for(let y=0;y<g;y++)f[y]!==void 0&&h++;return h===g}function l(f){let h=f.target;h.removeEventListener("dispose",l);let g=e.get(h);g!==void 0&&(e.delete(h),g.dispose())}function u(f){let h=f.target;h.removeEventListener("dispose",u);let g=t.get(h);g!==void 0&&(t.delete(h),g.dispose())}function d(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:d}}function mF(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&jf("WebGLRenderer: "+i+" extension not supported."),r}}}function gF(n,e,t,i){let r={},s=new WeakMap;function o(d){let f=d.target;f.index!==null&&e.remove(f.index);for(let g in f.attributes)e.remove(f.attributes[g]);f.removeEventListener("dispose",o),delete r[f.id];let h=s.get(f);h&&(e.remove(h),s.delete(f)),i.releaseStatesOfGeometry(f),f.isInstancedBufferGeometry===!0&&delete f._maxInstanceCount,t.memory.geometries--}function a(d,f){return r[f.id]===!0||(f.addEventListener("dispose",o),r[f.id]=!0,t.memory.geometries++),f}function c(d){let f=d.attributes;for(let h in f)e.update(f[h],n.ARRAY_BUFFER)}function l(d){let f=[],h=d.index,g=d.attributes.position,y=0;if(g===void 0)return;if(h!==null){let b=h.array;y=h.version;for(let M=0,S=b.length;M<S;M+=3){let A=b[M+0],w=b[M+1],I=b[M+2];f.push(A,w,w,I,I,A)}}else{let b=g.array;y=g.version;for(let M=0,S=b.length/3-1;M<S;M+=3){let A=M+0,w=M+1,I=M+2;f.push(A,w,w,I,I,A)}}let p=new(g.count>=65535?rl:il)(f,1);p.version=y;let m=s.get(d);m&&e.remove(m),s.set(d,p)}function u(d){let f=s.get(d);if(f){let h=d.index;h!==null&&f.version<h.version&&l(d)}else l(d);return s.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function vF(n,e,t){let i;function r(d){i=d}let s,o;function a(d){s=d.type,o=d.bytesPerElement}function c(d,f){n.drawElements(i,f,s,d*o),t.update(f,i,1)}function l(d,f,h){h!==0&&(n.drawElementsInstanced(i,f,s,d*o,h),t.update(f,i,h))}function u(d,f,h){if(h===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,d,0,h);let y=0;for(let p=0;p<h;p++)y+=f[p];t.update(y,i,1)}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u}function yF(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Pe("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function _F(n,e,t){let i=new WeakMap,r=new it;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let O=function(){_.dispose(),i.delete(a),a.removeEventListener("dispose",O)};var h=O;f!==void 0&&f.texture.dispose();let g=a.morphAttributes.position!==void 0,y=a.morphAttributes.normal!==void 0,p=a.morphAttributes.color!==void 0,m=a.morphAttributes.position||[],b=a.morphAttributes.normal||[],M=a.morphAttributes.color||[],S=0;g===!0&&(S=1),y===!0&&(S=2),p===!0&&(S=3);let A=a.attributes.position.count*S,w=1;A>e.maxTextureSize&&(w=Math.ceil(A/e.maxTextureSize),A=e.maxTextureSize);let I=new Float32Array(A*w*4*d),_=new tl(I,A,w,d);_.type=Ri,_.needsUpdate=!0;let C=S*4;for(let D=0;D<d;D++){let L=m[D],G=b[D],W=M[D],P=A*w*4*D;for(let H=0;H<L.count;H++){let U=H*C;g===!0&&(r.fromBufferAttribute(L,H),I[P+U+0]=r.x,I[P+U+1]=r.y,I[P+U+2]=r.z,I[P+U+3]=0),y===!0&&(r.fromBufferAttribute(G,H),I[P+U+4]=r.x,I[P+U+5]=r.y,I[P+U+6]=r.z,I[P+U+7]=0),p===!0&&(r.fromBufferAttribute(W,H),I[P+U+8]=r.x,I[P+U+9]=r.y,I[P+U+10]=r.z,I[P+U+11]=W.itemSize===4?r.w:1)}}f={count:d,texture:_,size:new Te(A,w)},i.set(a,f),a.addEventListener("dispose",O)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let p=0;p<l.length;p++)g+=l[p];let y=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",y),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:s}}function xF(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,d=l.geometry,f=e.get(l,d);if(s.get(f)!==u&&(e.update(f),s.set(f,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return f}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var bF={[Ey]:"LINEAR_TONE_MAPPING",[Sy]:"REINHARD_TONE_MAPPING",[My]:"CINEON_TONE_MAPPING",[wy]:"ACES_FILMIC_TONE_MAPPING",[Ty]:"AGX_TONE_MAPPING",[Dy]:"NEUTRAL_TONE_MAPPING",[Cy]:"CUSTOM_TONE_MAPPING"};function EF(n,e,t,i,r){let s=new Kn(e,t,{type:n,depthBuffer:i,stencilBuffer:r,depthTexture:i?new Nr(e,t):void 0}),o=new Kn(e,t,{type:nr,depthBuffer:!1,stencilBuffer:!1}),a=new Wt;a.setAttribute("position",new ut([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new ut([0,2,0,0,2,0],2));let c=new eh({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new tn(a,c),u=new Fa(-1,1,1,-1,0,1),d=null,f=null,h=!1,g,y=null,p=[],m=!1;this.setSize=function(b,M){s.setSize(b,M),o.setSize(b,M);for(let S=0;S<p.length;S++){let A=p[S];A.setSize&&A.setSize(b,M)}},this.setEffects=function(b){p=b,m=p.length>0&&p[0].isRenderPass===!0;let M=s.width,S=s.height;for(let A=0;A<p.length;A++){let w=p[A];w.setSize&&w.setSize(M,S)}},this.begin=function(b,M){if(h||b.toneMapping===Ai&&p.length===0)return!1;if(y=M,M!==null){let S=M.width,A=M.height;(s.width!==S||s.height!==A)&&this.setSize(S,A)}return m===!1&&b.setRenderTarget(s),g=b.toneMapping,b.toneMapping=Ai,!0},this.hasRenderPass=function(){return m},this.end=function(b,M){b.toneMapping=g,h=!0;let S=s,A=o;for(let w=0;w<p.length;w++){let I=p[w];if(I.enabled!==!1&&(I.render(b,A,S,M),I.needsSwap!==!1)){let _=S;S=A,A=_}}if(d!==b.outputColorSpace||f!==b.toneMapping){d=b.outputColorSpace,f=b.toneMapping,c.defines={},at.getTransfer(d)===pt&&(c.defines.SRGB_TRANSFER="");let w=bF[f];w&&(c.defines[w]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=S.texture,b.setRenderTarget(y),b.render(l,u),y=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){s.depthTexture&&s.depthTexture.dispose(),s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var RM=new ir,Xy=new Nr(1,1),NM=new tl,PM=new qf,OM=new cl,fM=[],hM=[],pM=new Float32Array(16),mM=new Float32Array(9),gM=new Float32Array(4);function Ga(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=fM[r];if(s===void 0&&(s=new Float32Array(r),fM[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function nn(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function rn(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function cp(n,e){let t=hM[e];t===void 0&&(t=new Int32Array(e),hM[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function SF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function MF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(nn(t,e))return;n.uniform2fv(this.addr,e),rn(t,e)}}function wF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(nn(t,e))return;n.uniform3fv(this.addr,e),rn(t,e)}}function CF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(nn(t,e))return;n.uniform4fv(this.addr,e),rn(t,e)}}function TF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(nn(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),rn(t,e)}else{if(nn(t,i))return;gM.set(i),n.uniformMatrix2fv(this.addr,!1,gM),rn(t,i)}}function DF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(nn(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),rn(t,e)}else{if(nn(t,i))return;mM.set(i),n.uniformMatrix3fv(this.addr,!1,mM),rn(t,i)}}function AF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(nn(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),rn(t,e)}else{if(nn(t,i))return;pM.set(i),n.uniformMatrix4fv(this.addr,!1,pM),rn(t,i)}}function IF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function RF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(nn(t,e))return;n.uniform2iv(this.addr,e),rn(t,e)}}function NF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(nn(t,e))return;n.uniform3iv(this.addr,e),rn(t,e)}}function PF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(nn(t,e))return;n.uniform4iv(this.addr,e),rn(t,e)}}function OF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function FF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(nn(t,e))return;n.uniform2uiv(this.addr,e),rn(t,e)}}function LF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(nn(t,e))return;n.uniform3uiv(this.addr,e),rn(t,e)}}function kF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(nn(t,e))return;n.uniform4uiv(this.addr,e),rn(t,e)}}function UF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(Xy.compareFunction=t.isReversedDepthBuffer()?tp:ep,s=Xy):s=RM,t.setTexture2D(e||s,r)}function BF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||PM,r)}function VF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||OM,r)}function HF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||NM,r)}function zF(n){switch(n){case 5126:return SF;case 35664:return MF;case 35665:return wF;case 35666:return CF;case 35674:return TF;case 35675:return DF;case 35676:return AF;case 5124:case 35670:return IF;case 35667:case 35671:return RF;case 35668:case 35672:return NF;case 35669:case 35673:return PF;case 5125:return OF;case 36294:return FF;case 36295:return LF;case 36296:return kF;case 35678:case 36198:case 36298:case 36306:case 35682:return UF;case 35679:case 36299:case 36307:return BF;case 35680:case 36300:case 36308:case 36293:return VF;case 36289:case 36303:case 36311:case 36292:return HF}}function GF(n,e){n.uniform1fv(this.addr,e)}function jF(n,e){let t=Ga(e,this.size,2);n.uniform2fv(this.addr,t)}function WF(n,e){let t=Ga(e,this.size,3);n.uniform3fv(this.addr,t)}function $F(n,e){let t=Ga(e,this.size,4);n.uniform4fv(this.addr,t)}function qF(n,e){let t=Ga(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function XF(n,e){let t=Ga(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function YF(n,e){let t=Ga(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function ZF(n,e){n.uniform1iv(this.addr,e)}function KF(n,e){n.uniform2iv(this.addr,e)}function JF(n,e){n.uniform3iv(this.addr,e)}function QF(n,e){n.uniform4iv(this.addr,e)}function eL(n,e){n.uniform1uiv(this.addr,e)}function tL(n,e){n.uniform2uiv(this.addr,e)}function nL(n,e){n.uniform3uiv(this.addr,e)}function iL(n,e){n.uniform4uiv(this.addr,e)}function rL(n,e,t){let i=this.cache,r=e.length,s=cp(t,r);nn(i,s)||(n.uniform1iv(this.addr,s),rn(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=Xy:o=RM;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function sL(n,e,t){let i=this.cache,r=e.length,s=cp(t,r);nn(i,s)||(n.uniform1iv(this.addr,s),rn(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||PM,s[o])}function oL(n,e,t){let i=this.cache,r=e.length,s=cp(t,r);nn(i,s)||(n.uniform1iv(this.addr,s),rn(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||OM,s[o])}function aL(n,e,t){let i=this.cache,r=e.length,s=cp(t,r);nn(i,s)||(n.uniform1iv(this.addr,s),rn(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||NM,s[o])}function cL(n){switch(n){case 5126:return GF;case 35664:return jF;case 35665:return WF;case 35666:return $F;case 35674:return qF;case 35675:return XF;case 35676:return YF;case 5124:case 35670:return ZF;case 35667:case 35671:return KF;case 35668:case 35672:return JF;case 35669:case 35673:return QF;case 5125:return eL;case 36294:return tL;case 36295:return nL;case 36296:return iL;case 35678:case 36198:case 36298:case 36306:case 35682:return rL;case 35679:case 36299:case 36307:return sL;case 35680:case 36300:case 36308:case 36293:return oL;case 36289:case 36303:case 36311:case 36292:return aL}}var Yy=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=zF(t.type)}},Zy=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=cL(t.type)}},Ky=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},$y=/(\w+)(\])?(\[|\.)?/g;function vM(n,e){n.seq.push(e),n.map[e.id]=e}function lL(n,e,t){let i=n.name,r=i.length;for($y.lastIndex=0;;){let s=$y.exec(i),o=$y.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){vM(t,l===void 0?new Yy(a,n,e):new Zy(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new Ky(a),vM(t,d)),t=d}}}var za=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);lL(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function yM(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var uL=37297,dL=0;function fL(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var _M=new je;function hL(n){at._getMatrix(_M,at.workingColorSpace,n);let e=`mat3( ${_M.elements.map(t=>t.toFixed(4))} )`;switch(at.getTransfer(n)){case Jc:return[e,"LinearTransferOETF"];case pt:return[e,"sRGBTransferOETF"];default:return Re("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function xM(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+fL(n.getShaderSource(e),a)}else return s}function pL(n,e){let t=hL(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var mL={[Ey]:"Linear",[Sy]:"Reinhard",[My]:"Cineon",[wy]:"ACESFilmic",[Ty]:"AgX",[Dy]:"Neutral",[Cy]:"Custom"};function gL(n,e){let t=mL[e];return t===void 0?(Re("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var ip=new T;function vL(){at.getLuminanceCoefficients(ip);let n=ip.x.toFixed(4),e=ip.y.toFixed(4),t=ip.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function yL(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Fl).join(`
`)}function _L(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function xL(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function Fl(n){return n!==""}function bM(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function EM(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var bL=/^[ \t]*#include +<([\w\d./]+)>/gm;function Jy(n){return n.replace(bL,SL)}var EL=new Map;function SL(n,e){let t=Je[e];if(t===void 0){let i=EL.get(e);if(i!==void 0)t=Je[i],Re('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Jy(t)}var ML=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function SM(n){return n.replace(ML,wL)}function wL(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function MM(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var CL={[Sl]:"SHADOWMAP_TYPE_PCF",[Ua]:"SHADOWMAP_TYPE_VSM"};function TL(n){return CL[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var DL={[ys]:"ENVMAP_TYPE_CUBE",[ao]:"ENVMAP_TYPE_CUBE",[Ml]:"ENVMAP_TYPE_CUBE_UV"};function AL(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":DL[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var IL={[ao]:"ENVMAP_MODE_REFRACTION"};function RL(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":IL[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var NL={[by]:"ENVMAP_BLENDING_MULTIPLY",[jS]:"ENVMAP_BLENDING_MIX",[WS]:"ENVMAP_BLENDING_ADD"};function PL(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":NL[n.combine]||"ENVMAP_BLENDING_NONE"}function OL(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function FL(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=TL(t),l=AL(t),u=RL(t),d=PL(t),f=OL(t),h=yL(t),g=_L(s),y=r.createProgram(),p,m,b=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Fl).join(`
`),p.length>0&&(p+=`
`),m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Fl).join(`
`),m.length>0&&(m+=`
`)):(p=[MM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexNormals?"#define HAS_NORMAL":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Fl).join(`
`),m=[MM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",f?"#define CUBEUV_TEXEL_WIDTH "+f.texelWidth:"",f?"#define CUBEUV_TEXEL_HEIGHT "+f.texelHeight:"",f?"#define CUBEUV_MAX_MIP "+f.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.packedNormalMap?"#define USE_PACKED_NORMALMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.numLightProbeGrids>0?"#define USE_LIGHT_PROBES_GRID":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==Ai?"#define TONE_MAPPING":"",t.toneMapping!==Ai?Je.tonemapping_pars_fragment:"",t.toneMapping!==Ai?gL("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Je.colorspace_pars_fragment,pL("linearToOutputTexel",t.outputColorSpace),vL(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Fl).join(`
`)),o=Jy(o),o=bM(o,t),o=EM(o,t),a=Jy(a),a=bM(a,t),a=EM(a,t),o=SM(o),a=SM(a),t.isRawShaderMaterial!==!0&&(b=`#version 300 es
`,p=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+p,m=["#define varying in",t.glslVersion===Ly?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===Ly?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+m);let M=b+p+o,S=b+m+a,A=yM(r,r.VERTEX_SHADER,M),w=yM(r,r.FRAGMENT_SHADER,S);r.attachShader(y,A),r.attachShader(y,w),t.index0AttributeName!==void 0?r.bindAttribLocation(y,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(y,0,"position"),r.linkProgram(y);function I(D){if(n.debug.checkShaderErrors){let L=r.getProgramInfoLog(y)||"",G=r.getShaderInfoLog(A)||"",W=r.getShaderInfoLog(w)||"",P=L.trim(),H=G.trim(),U=W.trim(),J=!0,Q=!0;if(r.getProgramParameter(y,r.LINK_STATUS)===!1)if(J=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,y,A,w);else{let ce=xM(r,A,"vertex"),xe=xM(r,w,"fragment");Pe("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(y,r.VALIDATE_STATUS)+`

Material Name: `+D.name+`
Material Type: `+D.type+`

Program Info Log: `+P+`
`+ce+`
`+xe)}else P!==""?Re("WebGLProgram: Program Info Log:",P):(H===""||U==="")&&(Q=!1);Q&&(D.diagnostics={runnable:J,programLog:P,vertexShader:{log:H,prefix:p},fragmentShader:{log:U,prefix:m}})}r.deleteShader(A),r.deleteShader(w),_=new za(r,y),C=xL(r,y)}let _;this.getUniforms=function(){return _===void 0&&I(this),_};let C;this.getAttributes=function(){return C===void 0&&I(this),C};let O=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return O===!1&&(O=r.getProgramParameter(y,uL)),O},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(y),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=dL++,this.cacheKey=e,this.usedTimes=1,this.program=y,this.vertexShader=A,this.fragmentShader=w,this}var LL=0,Qy=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new e_(e),t.set(e,i)),i}},e_=class{constructor(e){this.id=LL++,this.code=e,this.usedTimes=0}};function kL(n){return n===bs||n===Il||n===Rl}function UL(n,e,t,i,r,s){let o=new Ta,a=new Qy,c=new Set,l=[],u=new Map,d=i.logarithmicDepthBuffer,f=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function g(_){return c.add(_),_===0?"uv":`uv${_}`}function y(_,C,O,D,L,G){let W=D.fog,P=L.geometry,H=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?D.environment:null,U=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap,J=e.get(_.envMap||H,U),Q=J&&J.mapping===Ml?J.image.height:null,ce=h[_.type];_.precision!==null&&(f=i.getMaxPrecision(_.precision),f!==_.precision&&Re("WebGLProgram.getParameters:",_.precision,"not supported, using",f,"instead."));let xe=P.morphAttributes.position||P.morphAttributes.normal||P.morphAttributes.color,Se=xe!==void 0?xe.length:0,rt=0;P.morphAttributes.position!==void 0&&(rt=1),P.morphAttributes.normal!==void 0&&(rt=2),P.morphAttributes.color!==void 0&&(rt=3);let Ke,Fe,X,le;if(ce){let $e=On[ce];Ke=$e.vertexShader,Fe=$e.fragmentShader}else Ke=_.vertexShader,Fe=_.fragmentShader,a.update(_),X=a.getVertexShaderID(_),le=a.getFragmentShaderID(_);let ee=n.getRenderTarget(),Oe=n.state.buffers.depth.getReversed(),We=L.isInstancedMesh===!0,Le=L.isBatchedMesh===!0,Lt=!!_.map,st=!!_.matcap,vt=!!J,Ot=!!_.aoMap,tt=!!_.lightMap,Kt=!!_.bumpMap,kt=!!_.normalMap,jn=!!_.displacementMap,N=!!_.emissiveMap,Jt=!!_.metalnessMap,ot=!!_.roughnessMap,At=_.anisotropy>0,ue=_.clearcoat>0,Vt=_.dispersion>0,E=_.iridescence>0,v=_.sheen>0,k=_.transmission>0,q=At&&!!_.anisotropyMap,K=ue&&!!_.clearcoatMap,te=ue&&!!_.clearcoatNormalMap,oe=ue&&!!_.clearcoatRoughnessMap,j=E&&!!_.iridescenceMap,Y=E&&!!_.iridescenceThicknessMap,he=v&&!!_.sheenColorMap,ye=v&&!!_.sheenRoughnessMap,re=!!_.specularMap,ne=!!_.specularColorMap,He=!!_.specularIntensityMap,Xe=k&&!!_.transmissionMap,dt=k&&!!_.thicknessMap,R=!!_.gradientMap,ie=!!_.alphaMap,$=_.alphaTest>0,me=!!_.alphaHash,se=!!_.extensions,Z=Ai;_.toneMapped&&(ee===null||ee.isXRRenderTarget===!0)&&(Z=n.toneMapping);let Me={shaderID:ce,shaderType:_.type,shaderName:_.name,vertexShader:Ke,fragmentShader:Fe,defines:_.defines,customVertexShaderID:X,customFragmentShaderID:le,isRawShaderMaterial:_.isRawShaderMaterial===!0,glslVersion:_.glslVersion,precision:f,batching:Le,batchingColor:Le&&L._colorsTexture!==null,instancing:We,instancingColor:We&&L.instanceColor!==null,instancingMorph:We&&L.morphTexture!==null,outputColorSpace:ee===null?n.outputColorSpace:ee.isXRRenderTarget===!0?ee.texture.colorSpace:at.workingColorSpace,alphaToCoverage:!!_.alphaToCoverage,map:Lt,matcap:st,envMap:vt,envMapMode:vt&&J.mapping,envMapCubeUVHeight:Q,aoMap:Ot,lightMap:tt,bumpMap:Kt,normalMap:kt,displacementMap:jn,emissiveMap:N,normalMapObjectSpace:kt&&_.normalMapType===XS,normalMapTangentSpace:kt&&_.normalMapType===Qh,packedNormalMap:kt&&_.normalMapType===Qh&&kL(_.normalMap.format),metalnessMap:Jt,roughnessMap:ot,anisotropy:At,anisotropyMap:q,clearcoat:ue,clearcoatMap:K,clearcoatNormalMap:te,clearcoatRoughnessMap:oe,dispersion:Vt,iridescence:E,iridescenceMap:j,iridescenceThicknessMap:Y,sheen:v,sheenColorMap:he,sheenRoughnessMap:ye,specularMap:re,specularColorMap:ne,specularIntensityMap:He,transmission:k,transmissionMap:Xe,thicknessMap:dt,gradientMap:R,opaque:_.transparent===!1&&_.blending===so&&_.alphaToCoverage===!1,alphaMap:ie,alphaTest:$,alphaHash:me,combine:_.combine,mapUv:Lt&&g(_.map.channel),aoMapUv:Ot&&g(_.aoMap.channel),lightMapUv:tt&&g(_.lightMap.channel),bumpMapUv:Kt&&g(_.bumpMap.channel),normalMapUv:kt&&g(_.normalMap.channel),displacementMapUv:jn&&g(_.displacementMap.channel),emissiveMapUv:N&&g(_.emissiveMap.channel),metalnessMapUv:Jt&&g(_.metalnessMap.channel),roughnessMapUv:ot&&g(_.roughnessMap.channel),anisotropyMapUv:q&&g(_.anisotropyMap.channel),clearcoatMapUv:K&&g(_.clearcoatMap.channel),clearcoatNormalMapUv:te&&g(_.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:oe&&g(_.clearcoatRoughnessMap.channel),iridescenceMapUv:j&&g(_.iridescenceMap.channel),iridescenceThicknessMapUv:Y&&g(_.iridescenceThicknessMap.channel),sheenColorMapUv:he&&g(_.sheenColorMap.channel),sheenRoughnessMapUv:ye&&g(_.sheenRoughnessMap.channel),specularMapUv:re&&g(_.specularMap.channel),specularColorMapUv:ne&&g(_.specularColorMap.channel),specularIntensityMapUv:He&&g(_.specularIntensityMap.channel),transmissionMapUv:Xe&&g(_.transmissionMap.channel),thicknessMapUv:dt&&g(_.thicknessMap.channel),alphaMapUv:ie&&g(_.alphaMap.channel),vertexTangents:!!P.attributes.tangent&&(kt||At),vertexNormals:!!P.attributes.normal,vertexColors:_.vertexColors,vertexAlphas:_.vertexColors===!0&&!!P.attributes.color&&P.attributes.color.itemSize===4,pointsUvs:L.isPoints===!0&&!!P.attributes.uv&&(Lt||ie),fog:!!W,useFog:_.fog===!0,fogExp2:!!W&&W.isFogExp2,flatShading:_.wireframe===!1&&(_.flatShading===!0||P.attributes.normal===void 0&&kt===!1&&(_.isMeshLambertMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isMeshPhysicalMaterial)),sizeAttenuation:_.sizeAttenuation===!0,logarithmicDepthBuffer:d,reversedDepthBuffer:Oe,skinning:L.isSkinnedMesh===!0,morphTargets:P.morphAttributes.position!==void 0,morphNormals:P.morphAttributes.normal!==void 0,morphColors:P.morphAttributes.color!==void 0,morphTargetsCount:Se,morphTextureStride:rt,numDirLights:C.directional.length,numPointLights:C.point.length,numSpotLights:C.spot.length,numSpotLightMaps:C.spotLightMap.length,numRectAreaLights:C.rectArea.length,numHemiLights:C.hemi.length,numDirLightShadows:C.directionalShadowMap.length,numPointLightShadows:C.pointShadowMap.length,numSpotLightShadows:C.spotShadowMap.length,numSpotLightShadowsWithMaps:C.numSpotLightShadowsWithMaps,numLightProbes:C.numLightProbes,numLightProbeGrids:G.length,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:_.dithering,shadowMapEnabled:n.shadowMap.enabled&&O.length>0,shadowMapType:n.shadowMap.type,toneMapping:Z,decodeVideoTexture:Lt&&_.map.isVideoTexture===!0&&at.getTransfer(_.map.colorSpace)===pt,decodeVideoTextureEmissive:N&&_.emissiveMap.isVideoTexture===!0&&at.getTransfer(_.emissiveMap.colorSpace)===pt,premultipliedAlpha:_.premultipliedAlpha,doubleSided:_.side===fi,flipSided:_.side===Pn,useDepthPacking:_.depthPacking>=0,depthPacking:_.depthPacking||0,index0AttributeName:_.index0AttributeName,extensionClipCullDistance:se&&_.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(se&&_.extensions.multiDraw===!0||Le)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:_.customProgramCacheKey()};return Me.vertexUv1s=c.has(1),Me.vertexUv2s=c.has(2),Me.vertexUv3s=c.has(3),c.clear(),Me}function p(_){let C=[];if(_.shaderID?C.push(_.shaderID):(C.push(_.customVertexShaderID),C.push(_.customFragmentShaderID)),_.defines!==void 0)for(let O in _.defines)C.push(O),C.push(_.defines[O]);return _.isRawShaderMaterial===!1&&(m(C,_),b(C,_),C.push(n.outputColorSpace)),C.push(_.customProgramCacheKey),C.join()}function m(_,C){_.push(C.precision),_.push(C.outputColorSpace),_.push(C.envMapMode),_.push(C.envMapCubeUVHeight),_.push(C.mapUv),_.push(C.alphaMapUv),_.push(C.lightMapUv),_.push(C.aoMapUv),_.push(C.bumpMapUv),_.push(C.normalMapUv),_.push(C.displacementMapUv),_.push(C.emissiveMapUv),_.push(C.metalnessMapUv),_.push(C.roughnessMapUv),_.push(C.anisotropyMapUv),_.push(C.clearcoatMapUv),_.push(C.clearcoatNormalMapUv),_.push(C.clearcoatRoughnessMapUv),_.push(C.iridescenceMapUv),_.push(C.iridescenceThicknessMapUv),_.push(C.sheenColorMapUv),_.push(C.sheenRoughnessMapUv),_.push(C.specularMapUv),_.push(C.specularColorMapUv),_.push(C.specularIntensityMapUv),_.push(C.transmissionMapUv),_.push(C.thicknessMapUv),_.push(C.combine),_.push(C.fogExp2),_.push(C.sizeAttenuation),_.push(C.morphTargetsCount),_.push(C.morphAttributeCount),_.push(C.numDirLights),_.push(C.numPointLights),_.push(C.numSpotLights),_.push(C.numSpotLightMaps),_.push(C.numHemiLights),_.push(C.numRectAreaLights),_.push(C.numDirLightShadows),_.push(C.numPointLightShadows),_.push(C.numSpotLightShadows),_.push(C.numSpotLightShadowsWithMaps),_.push(C.numLightProbes),_.push(C.shadowMapType),_.push(C.toneMapping),_.push(C.numClippingPlanes),_.push(C.numClipIntersection),_.push(C.depthPacking)}function b(_,C){o.disableAll(),C.instancing&&o.enable(0),C.instancingColor&&o.enable(1),C.instancingMorph&&o.enable(2),C.matcap&&o.enable(3),C.envMap&&o.enable(4),C.normalMapObjectSpace&&o.enable(5),C.normalMapTangentSpace&&o.enable(6),C.clearcoat&&o.enable(7),C.iridescence&&o.enable(8),C.alphaTest&&o.enable(9),C.vertexColors&&o.enable(10),C.vertexAlphas&&o.enable(11),C.vertexUv1s&&o.enable(12),C.vertexUv2s&&o.enable(13),C.vertexUv3s&&o.enable(14),C.vertexTangents&&o.enable(15),C.anisotropy&&o.enable(16),C.alphaHash&&o.enable(17),C.batching&&o.enable(18),C.dispersion&&o.enable(19),C.batchingColor&&o.enable(20),C.gradientMap&&o.enable(21),C.packedNormalMap&&o.enable(22),C.vertexNormals&&o.enable(23),_.push(o.mask),o.disableAll(),C.fog&&o.enable(0),C.useFog&&o.enable(1),C.flatShading&&o.enable(2),C.logarithmicDepthBuffer&&o.enable(3),C.reversedDepthBuffer&&o.enable(4),C.skinning&&o.enable(5),C.morphTargets&&o.enable(6),C.morphNormals&&o.enable(7),C.morphColors&&o.enable(8),C.premultipliedAlpha&&o.enable(9),C.shadowMapEnabled&&o.enable(10),C.doubleSided&&o.enable(11),C.flipSided&&o.enable(12),C.useDepthPacking&&o.enable(13),C.dithering&&o.enable(14),C.transmission&&o.enable(15),C.sheen&&o.enable(16),C.opaque&&o.enable(17),C.pointsUvs&&o.enable(18),C.decodeVideoTexture&&o.enable(19),C.decodeVideoTextureEmissive&&o.enable(20),C.alphaToCoverage&&o.enable(21),C.numLightProbeGrids>0&&o.enable(22),_.push(o.mask)}function M(_){let C=h[_.type],O;if(C){let D=On[C];O=Nl.clone(D.uniforms)}else O=_.uniforms;return O}function S(_,C){let O=u.get(C);return O!==void 0?++O.usedTimes:(O=new FL(n,C,_,r),l.push(O),u.set(C,O)),O}function A(_){if(--_.usedTimes===0){let C=l.indexOf(_);l[C]=l[l.length-1],l.pop(),u.delete(_.cacheKey),_.destroy()}}function w(_){a.remove(_)}function I(){a.dispose()}return{getParameters:y,getProgramCacheKey:p,getUniforms:M,acquireProgram:S,releaseProgram:A,releaseShaderCache:w,programs:l,dispose:I}}function BL(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function VL(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function wM(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function CM(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(f){let h=0;return f.isInstancedMesh&&(h+=2),f.isSkinnedMesh&&(h+=1),h}function a(f,h,g,y,p,m){let b=n[e];return b===void 0?(b={id:f.id,object:f,geometry:h,material:g,materialVariant:o(f),groupOrder:y,renderOrder:f.renderOrder,z:p,group:m},n[e]=b):(b.id=f.id,b.object=f,b.geometry=h,b.material=g,b.materialVariant=o(f),b.groupOrder=y,b.renderOrder=f.renderOrder,b.z=p,b.group=m),e++,b}function c(f,h,g,y,p,m){let b=a(f,h,g,y,p,m);g.transmission>0?i.push(b):g.transparent===!0?r.push(b):t.push(b)}function l(f,h,g,y,p,m){let b=a(f,h,g,y,p,m);g.transmission>0?i.unshift(b):g.transparent===!0?r.unshift(b):t.unshift(b)}function u(f,h){t.length>1&&t.sort(f||VL),i.length>1&&i.sort(h||wM),r.length>1&&r.sort(h||wM)}function d(){for(let f=e,h=n.length;f<h;f++){let g=n[f];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:d,sort:u}}function HL(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new CM,n.set(i,[o])):r>=s.length?(o=new CM,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function zL(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new T,color:new Ve};break;case"SpotLight":t={position:new T,direction:new T,color:new Ve,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new T,color:new Ve,distance:0,decay:0};break;case"HemisphereLight":t={direction:new T,skyColor:new Ve,groundColor:new Ve};break;case"RectAreaLight":t={color:new Ve,position:new T,halfWidth:new T,halfHeight:new T};break}return n[e.id]=t,t}}}function GL(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Te};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Te};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Te,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var jL=0;function WL(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function $L(n){let e=new zL,t=GL(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new T);let r=new T,s=new Dt,o=new Dt;function a(l){let u=0,d=0,f=0;for(let C=0;C<9;C++)i.probe[C].set(0,0,0);let h=0,g=0,y=0,p=0,m=0,b=0,M=0,S=0,A=0,w=0,I=0;l.sort(WL);for(let C=0,O=l.length;C<O;C++){let D=l[C],L=D.color,G=D.intensity,W=D.distance,P=null;if(D.shadow&&D.shadow.map&&(D.shadow.map.texture.format===bs?P=D.shadow.map.texture:P=D.shadow.map.depthTexture||D.shadow.map.texture),D.isAmbientLight)u+=L.r*G,d+=L.g*G,f+=L.b*G;else if(D.isLightProbe){for(let H=0;H<9;H++)i.probe[H].addScaledVector(D.sh.coefficients[H],G);I++}else if(D.isDirectionalLight){let H=e.get(D);if(H.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let U=D.shadow,J=t.get(D);J.shadowIntensity=U.intensity,J.shadowBias=U.bias,J.shadowNormalBias=U.normalBias,J.shadowRadius=U.radius,J.shadowMapSize=U.mapSize,i.directionalShadow[h]=J,i.directionalShadowMap[h]=P,i.directionalShadowMatrix[h]=D.shadow.matrix,b++}i.directional[h]=H,h++}else if(D.isSpotLight){let H=e.get(D);H.position.setFromMatrixPosition(D.matrixWorld),H.color.copy(L).multiplyScalar(G),H.distance=W,H.coneCos=Math.cos(D.angle),H.penumbraCos=Math.cos(D.angle*(1-D.penumbra)),H.decay=D.decay,i.spot[y]=H;let U=D.shadow;if(D.map&&(i.spotLightMap[A]=D.map,A++,U.updateMatrices(D),D.castShadow&&w++),i.spotLightMatrix[y]=U.matrix,D.castShadow){let J=t.get(D);J.shadowIntensity=U.intensity,J.shadowBias=U.bias,J.shadowNormalBias=U.normalBias,J.shadowRadius=U.radius,J.shadowMapSize=U.mapSize,i.spotShadow[y]=J,i.spotShadowMap[y]=P,S++}y++}else if(D.isRectAreaLight){let H=e.get(D);H.color.copy(L).multiplyScalar(G),H.halfWidth.set(D.width*.5,0,0),H.halfHeight.set(0,D.height*.5,0),i.rectArea[p]=H,p++}else if(D.isPointLight){let H=e.get(D);if(H.color.copy(D.color).multiplyScalar(D.intensity),H.distance=D.distance,H.decay=D.decay,D.castShadow){let U=D.shadow,J=t.get(D);J.shadowIntensity=U.intensity,J.shadowBias=U.bias,J.shadowNormalBias=U.normalBias,J.shadowRadius=U.radius,J.shadowMapSize=U.mapSize,J.shadowCameraNear=U.camera.near,J.shadowCameraFar=U.camera.far,i.pointShadow[g]=J,i.pointShadowMap[g]=P,i.pointShadowMatrix[g]=D.shadow.matrix,M++}i.point[g]=H,g++}else if(D.isHemisphereLight){let H=e.get(D);H.skyColor.copy(D.color).multiplyScalar(G),H.groundColor.copy(D.groundColor).multiplyScalar(G),i.hemi[m]=H,m++}}p>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=ae.LTC_FLOAT_1,i.rectAreaLTC2=ae.LTC_FLOAT_2):(i.rectAreaLTC1=ae.LTC_HALF_1,i.rectAreaLTC2=ae.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let _=i.hash;(_.directionalLength!==h||_.pointLength!==g||_.spotLength!==y||_.rectAreaLength!==p||_.hemiLength!==m||_.numDirectionalShadows!==b||_.numPointShadows!==M||_.numSpotShadows!==S||_.numSpotMaps!==A||_.numLightProbes!==I)&&(i.directional.length=h,i.spot.length=y,i.rectArea.length=p,i.point.length=g,i.hemi.length=m,i.directionalShadow.length=b,i.directionalShadowMap.length=b,i.pointShadow.length=M,i.pointShadowMap.length=M,i.spotShadow.length=S,i.spotShadowMap.length=S,i.directionalShadowMatrix.length=b,i.pointShadowMatrix.length=M,i.spotLightMatrix.length=S+A-w,i.spotLightMap.length=A,i.numSpotLightShadowsWithMaps=w,i.numLightProbes=I,_.directionalLength=h,_.pointLength=g,_.spotLength=y,_.rectAreaLength=p,_.hemiLength=m,_.numDirectionalShadows=b,_.numPointShadows=M,_.numSpotShadows=S,_.numSpotMaps=A,_.numLightProbes=I,i.version=jL++)}function c(l,u){let d=0,f=0,h=0,g=0,y=0,p=u.matrixWorldInverse;for(let m=0,b=l.length;m<b;m++){let M=l[m];if(M.isDirectionalLight){let S=i.directional[d];S.direction.setFromMatrixPosition(M.matrixWorld),r.setFromMatrixPosition(M.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(p),d++}else if(M.isSpotLight){let S=i.spot[h];S.position.setFromMatrixPosition(M.matrixWorld),S.position.applyMatrix4(p),S.direction.setFromMatrixPosition(M.matrixWorld),r.setFromMatrixPosition(M.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(p),h++}else if(M.isRectAreaLight){let S=i.rectArea[g];S.position.setFromMatrixPosition(M.matrixWorld),S.position.applyMatrix4(p),o.identity(),s.copy(M.matrixWorld),s.premultiply(p),o.extractRotation(s),S.halfWidth.set(M.width*.5,0,0),S.halfHeight.set(0,M.height*.5,0),S.halfWidth.applyMatrix4(o),S.halfHeight.applyMatrix4(o),g++}else if(M.isPointLight){let S=i.point[f];S.position.setFromMatrixPosition(M.matrixWorld),S.position.applyMatrix4(p),f++}else if(M.isHemisphereLight){let S=i.hemi[y];S.direction.setFromMatrixPosition(M.matrixWorld),S.direction.transformDirection(p),y++}}}return{setup:a,setupView:c,state:i}}function TM(n){let e=new $L(n),t=[],i=[],r=[];function s(f){d.camera=f,t.length=0,i.length=0,r.length=0}function o(f){t.push(f)}function a(f){i.push(f)}function c(f){r.push(f)}function l(){e.setup(t)}function u(f){e.setupView(t,f)}let d={lightsArray:t,shadowsArray:i,lightProbeGridArray:r,camera:null,lights:e,transmissionRenderTarget:{},textureUnits:0};return{init:s,state:d,setupLights:l,setupLightsView:u,pushLight:o,pushShadow:a,pushLightProbeGrid:c}}function qL(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new TM(n),e.set(r,[a])):s>=o.length?(a=new TM(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var XL=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,YL=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,ZL=[new T(1,0,0),new T(-1,0,0),new T(0,1,0),new T(0,-1,0),new T(0,0,1),new T(0,0,-1)],KL=[new T(0,-1,0),new T(0,-1,0),new T(0,0,1),new T(0,0,-1),new T(0,-1,0),new T(0,-1,0)],DM=new Dt,Ol=new T,qy=new T;function JL(n,e,t){let i=new Ia,r=new Te,s=new Te,o=new it,a=new nh,c=new ih,l={},u=t.maxTextureSize,d={[Rr]:Pn,[Pn]:Rr,[fi]:fi},f=new Nn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new Te},radius:{value:4}},vertexShader:XL,fragmentShader:YL}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let g=new Wt;g.setAttribute("position",new Hn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let y=new tn(g,f),p=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=Sl;let m=this.type;this.render=function(w,I,_){if(p.enabled===!1||p.autoUpdate===!1&&p.needsUpdate===!1||w.length===0)return;this.type===wS&&(Re("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=Sl);let C=n.getRenderTarget(),O=n.getActiveCubeFace(),D=n.getActiveMipmapLevel(),L=n.state;L.setBlending(tr),L.buffers.depth.getReversed()===!0?L.buffers.color.setClear(0,0,0,0):L.buffers.color.setClear(1,1,1,1),L.buffers.depth.setTest(!0),L.setScissorTest(!1);let G=m!==this.type;G&&I.traverse(function(W){W.material&&(Array.isArray(W.material)?W.material.forEach(P=>P.needsUpdate=!0):W.material.needsUpdate=!0)});for(let W=0,P=w.length;W<P;W++){let H=w[W],U=H.shadow;if(U===void 0){Re("WebGLShadowMap:",H,"has no shadow.");continue}if(U.autoUpdate===!1&&U.needsUpdate===!1)continue;r.copy(U.mapSize);let J=U.getFrameExtents();r.multiply(J),s.copy(U.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/J.x),r.x=s.x*J.x,U.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/J.y),r.y=s.y*J.y,U.mapSize.y=s.y));let Q=n.state.buffers.depth.getReversed();if(U.camera._reversedDepth=Q,U.map===null||G===!0){if(U.map!==null&&(U.map.depthTexture!==null&&(U.map.depthTexture.dispose(),U.map.depthTexture=null),U.map.dispose()),this.type===Ua){if(H.isPointLight){Re("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}U.map=new Kn(r.x,r.y,{format:bs,type:nr,minFilter:hn,magFilter:hn,generateMipmaps:!1}),U.map.texture.name=H.name+".shadowMap",U.map.depthTexture=new Nr(r.x,r.y,Ri),U.map.depthTexture.name=H.name+".shadowMapDepth",U.map.depthTexture.format=Qi,U.map.depthTexture.compareFunction=null,U.map.depthTexture.minFilter=an,U.map.depthTexture.magFilter=an}else H.isPointLight?(U.map=new sp(r.x),U.map.depthTexture=new Kf(r.x,Ii)):(U.map=new Kn(r.x,r.y),U.map.depthTexture=new Nr(r.x,r.y,Ii)),U.map.depthTexture.name=H.name+".shadowMap",U.map.depthTexture.format=Qi,this.type===Sl?(U.map.depthTexture.compareFunction=Q?tp:ep,U.map.depthTexture.minFilter=hn,U.map.depthTexture.magFilter=hn):(U.map.depthTexture.compareFunction=null,U.map.depthTexture.minFilter=an,U.map.depthTexture.magFilter=an);U.camera.updateProjectionMatrix()}let ce=U.map.isWebGLCubeRenderTarget?6:1;for(let xe=0;xe<ce;xe++){if(U.map.isWebGLCubeRenderTarget)n.setRenderTarget(U.map,xe),n.clear();else{xe===0&&(n.setRenderTarget(U.map),n.clear());let Se=U.getViewport(xe);o.set(s.x*Se.x,s.y*Se.y,s.x*Se.z,s.y*Se.w),L.viewport(o)}if(H.isPointLight){let Se=U.camera,rt=U.matrix,Ke=H.distance||Se.far;Ke!==Se.far&&(Se.far=Ke,Se.updateProjectionMatrix()),Ol.setFromMatrixPosition(H.matrixWorld),Se.position.copy(Ol),qy.copy(Se.position),qy.add(ZL[xe]),Se.up.copy(KL[xe]),Se.lookAt(qy),Se.updateMatrixWorld(),rt.makeTranslation(-Ol.x,-Ol.y,-Ol.z),DM.multiplyMatrices(Se.projectionMatrix,Se.matrixWorldInverse),U._frustum.setFromProjectionMatrix(DM,Se.coordinateSystem,Se.reversedDepth)}else U.updateMatrices(H);i=U.getFrustum(),S(I,_,U.camera,H,this.type)}U.isPointLightShadow!==!0&&this.type===Ua&&b(U,_),U.needsUpdate=!1}m=this.type,p.needsUpdate=!1,n.setRenderTarget(C,O,D)};function b(w,I){let _=e.update(y);f.defines.VSM_SAMPLES!==w.blurSamples&&(f.defines.VSM_SAMPLES=w.blurSamples,h.defines.VSM_SAMPLES=w.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),w.mapPass===null&&(w.mapPass=new Kn(r.x,r.y,{format:bs,type:nr})),f.uniforms.shadow_pass.value=w.map.depthTexture,f.uniforms.resolution.value=w.mapSize,f.uniforms.radius.value=w.radius,n.setRenderTarget(w.mapPass),n.clear(),n.renderBufferDirect(I,null,_,f,y,null),h.uniforms.shadow_pass.value=w.mapPass.texture,h.uniforms.resolution.value=w.mapSize,h.uniforms.radius.value=w.radius,n.setRenderTarget(w.map),n.clear(),n.renderBufferDirect(I,null,_,h,y,null)}function M(w,I,_,C){let O=null,D=_.isPointLight===!0?w.customDistanceMaterial:w.customDepthMaterial;if(D!==void 0)O=D;else if(O=_.isPointLight===!0?c:a,n.localClippingEnabled&&I.clipShadows===!0&&Array.isArray(I.clippingPlanes)&&I.clippingPlanes.length!==0||I.displacementMap&&I.displacementScale!==0||I.alphaMap&&I.alphaTest>0||I.map&&I.alphaTest>0||I.alphaToCoverage===!0){let L=O.uuid,G=I.uuid,W=l[L];W===void 0&&(W={},l[L]=W);let P=W[G];P===void 0&&(P=O.clone(),W[G]=P,I.addEventListener("dispose",A)),O=P}if(O.visible=I.visible,O.wireframe=I.wireframe,C===Ua?O.side=I.shadowSide!==null?I.shadowSide:I.side:O.side=I.shadowSide!==null?I.shadowSide:d[I.side],O.alphaMap=I.alphaMap,O.alphaTest=I.alphaToCoverage===!0?.5:I.alphaTest,O.map=I.map,O.clipShadows=I.clipShadows,O.clippingPlanes=I.clippingPlanes,O.clipIntersection=I.clipIntersection,O.displacementMap=I.displacementMap,O.displacementScale=I.displacementScale,O.displacementBias=I.displacementBias,O.wireframeLinewidth=I.wireframeLinewidth,O.linewidth=I.linewidth,_.isPointLight===!0&&O.isMeshDistanceMaterial===!0){let L=n.properties.get(O);L.light=_}return O}function S(w,I,_,C,O){if(w.visible===!1)return;if(w.layers.test(I.layers)&&(w.isMesh||w.isLine||w.isPoints)&&(w.castShadow||w.receiveShadow&&O===Ua)&&(!w.frustumCulled||i.intersectsObject(w))){w.modelViewMatrix.multiplyMatrices(_.matrixWorldInverse,w.matrixWorld);let G=e.update(w),W=w.material;if(Array.isArray(W)){let P=G.groups;for(let H=0,U=P.length;H<U;H++){let J=P[H],Q=W[J.materialIndex];if(Q&&Q.visible){let ce=M(w,Q,C,O);w.onBeforeShadow(n,w,I,_,G,ce,J),n.renderBufferDirect(_,null,G,ce,w,J),w.onAfterShadow(n,w,I,_,G,ce,J)}}}else if(W.visible){let P=M(w,W,C,O);w.onBeforeShadow(n,w,I,_,G,P,null),n.renderBufferDirect(_,null,G,P,w,null),w.onAfterShadow(n,w,I,_,G,P,null)}}let L=w.children;for(let G=0,W=L.length;G<W;G++)S(L[G],I,_,C,O)}function A(w){w.target.removeEventListener("dispose",A);for(let _ in l){let C=l[_],O=w.target.uuid;O in C&&(C[O].dispose(),delete C[O])}}}function QL(n,e){function t(){let R=!1,ie=new it,$=null,me=new it(0,0,0,0);return{setMask:function(se){$!==se&&!R&&(n.colorMask(se,se,se,se),$=se)},setLocked:function(se){R=se},setClear:function(se,Z,Me,$e,$t){$t===!0&&(se*=$e,Z*=$e,Me*=$e),ie.set(se,Z,Me,$e),me.equals(ie)===!1&&(n.clearColor(se,Z,Me,$e),me.copy(ie))},reset:function(){R=!1,$=null,me.set(-1,0,0,0)}}}function i(){let R=!1,ie=!1,$=null,me=null,se=null;return{setReversed:function(Z){if(ie!==Z){let Me=e.get("EXT_clip_control");Z?Me.clipControlEXT(Me.LOWER_LEFT_EXT,Me.ZERO_TO_ONE_EXT):Me.clipControlEXT(Me.LOWER_LEFT_EXT,Me.NEGATIVE_ONE_TO_ONE_EXT),ie=Z;let $e=se;se=null,this.setClear($e)}},getReversed:function(){return ie},setTest:function(Z){Z?ee(n.DEPTH_TEST):Oe(n.DEPTH_TEST)},setMask:function(Z){$!==Z&&!R&&(n.depthMask(Z),$=Z)},setFunc:function(Z){if(ie&&(Z=rM[Z]),me!==Z){switch(Z){case Pf:n.depthFunc(n.NEVER);break;case Of:n.depthFunc(n.ALWAYS);break;case Ff:n.depthFunc(n.LESS);break;case oo:n.depthFunc(n.LEQUAL);break;case Lf:n.depthFunc(n.EQUAL);break;case kf:n.depthFunc(n.GEQUAL);break;case Uf:n.depthFunc(n.GREATER);break;case Bf:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}me=Z}},setLocked:function(Z){R=Z},setClear:function(Z){se!==Z&&(se=Z,ie&&(Z=1-Z),n.clearDepth(Z))},reset:function(){R=!1,$=null,me=null,se=null,ie=!1}}}function r(){let R=!1,ie=null,$=null,me=null,se=null,Z=null,Me=null,$e=null,$t=null;return{setTest:function(yt){R||(yt?ee(n.STENCIL_TEST):Oe(n.STENCIL_TEST))},setMask:function(yt){ie!==yt&&!R&&(n.stencilMask(yt),ie=yt)},setFunc:function(yt,cr,Pi){($!==yt||me!==cr||se!==Pi)&&(n.stencilFunc(yt,cr,Pi),$=yt,me=cr,se=Pi)},setOp:function(yt,cr,Pi){(Z!==yt||Me!==cr||$e!==Pi)&&(n.stencilOp(yt,cr,Pi),Z=yt,Me=cr,$e=Pi)},setLocked:function(yt){R=yt},setClear:function(yt){$t!==yt&&(n.clearStencil(yt),$t=yt)},reset:function(){R=!1,ie=null,$=null,me=null,se=null,Z=null,Me=null,$e=null,$t=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},f={},h=new WeakMap,g=[],y=null,p=!1,m=null,b=null,M=null,S=null,A=null,w=null,I=null,_=new Ve(0,0,0),C=0,O=!1,D=null,L=null,G=null,W=null,P=null,H=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),U=!1,J=0,Q=n.getParameter(n.VERSION);Q.indexOf("WebGL")!==-1?(J=parseFloat(/^WebGL (\d)/.exec(Q)[1]),U=J>=1):Q.indexOf("OpenGL ES")!==-1&&(J=parseFloat(/^OpenGL ES (\d)/.exec(Q)[1]),U=J>=2);let ce=null,xe={},Se=n.getParameter(n.SCISSOR_BOX),rt=n.getParameter(n.VIEWPORT),Ke=new it().fromArray(Se),Fe=new it().fromArray(rt);function X(R,ie,$,me){let se=new Uint8Array(4),Z=n.createTexture();n.bindTexture(R,Z),n.texParameteri(R,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(R,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Me=0;Me<$;Me++)R===n.TEXTURE_3D||R===n.TEXTURE_2D_ARRAY?n.texImage3D(ie,0,n.RGBA,1,1,me,0,n.RGBA,n.UNSIGNED_BYTE,se):n.texImage2D(ie+Me,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,se);return Z}let le={};le[n.TEXTURE_2D]=X(n.TEXTURE_2D,n.TEXTURE_2D,1),le[n.TEXTURE_CUBE_MAP]=X(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),le[n.TEXTURE_2D_ARRAY]=X(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),le[n.TEXTURE_3D]=X(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ee(n.DEPTH_TEST),o.setFunc(oo),Kt(!1),kt(vy),ee(n.CULL_FACE),Ot(tr);function ee(R){u[R]!==!0&&(n.enable(R),u[R]=!0)}function Oe(R){u[R]!==!1&&(n.disable(R),u[R]=!1)}function We(R,ie){return f[R]!==ie?(n.bindFramebuffer(R,ie),f[R]=ie,R===n.DRAW_FRAMEBUFFER&&(f[n.FRAMEBUFFER]=ie),R===n.FRAMEBUFFER&&(f[n.DRAW_FRAMEBUFFER]=ie),!0):!1}function Le(R,ie){let $=g,me=!1;if(R){$=h.get(ie),$===void 0&&($=[],h.set(ie,$));let se=R.textures;if($.length!==se.length||$[0]!==n.COLOR_ATTACHMENT0){for(let Z=0,Me=se.length;Z<Me;Z++)$[Z]=n.COLOR_ATTACHMENT0+Z;$.length=se.length,me=!0}}else $[0]!==n.BACK&&($[0]=n.BACK,me=!0);me&&n.drawBuffers($)}function Lt(R){return y!==R?(n.useProgram(R),y=R,!0):!1}let st={[cs]:n.FUNC_ADD,[TS]:n.FUNC_SUBTRACT,[DS]:n.FUNC_REVERSE_SUBTRACT};st[AS]=n.MIN,st[IS]=n.MAX;let vt={[RS]:n.ZERO,[NS]:n.ONE,[PS]:n.SRC_COLOR,[Rf]:n.SRC_ALPHA,[BS]:n.SRC_ALPHA_SATURATE,[kS]:n.DST_COLOR,[FS]:n.DST_ALPHA,[OS]:n.ONE_MINUS_SRC_COLOR,[Nf]:n.ONE_MINUS_SRC_ALPHA,[US]:n.ONE_MINUS_DST_COLOR,[LS]:n.ONE_MINUS_DST_ALPHA,[VS]:n.CONSTANT_COLOR,[HS]:n.ONE_MINUS_CONSTANT_COLOR,[zS]:n.CONSTANT_ALPHA,[GS]:n.ONE_MINUS_CONSTANT_ALPHA};function Ot(R,ie,$,me,se,Z,Me,$e,$t,yt){if(R===tr){p===!0&&(Oe(n.BLEND),p=!1);return}if(p===!1&&(ee(n.BLEND),p=!0),R!==CS){if(R!==m||yt!==O){if((b!==cs||A!==cs)&&(n.blendEquation(n.FUNC_ADD),b=cs,A=cs),yt)switch(R){case so:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case yy:n.blendFunc(n.ONE,n.ONE);break;case _y:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case xy:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Pe("WebGLState: Invalid blending: ",R);break}else switch(R){case so:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case yy:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case _y:Pe("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case xy:Pe("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Pe("WebGLState: Invalid blending: ",R);break}M=null,S=null,w=null,I=null,_.set(0,0,0),C=0,m=R,O=yt}return}se=se||ie,Z=Z||$,Me=Me||me,(ie!==b||se!==A)&&(n.blendEquationSeparate(st[ie],st[se]),b=ie,A=se),($!==M||me!==S||Z!==w||Me!==I)&&(n.blendFuncSeparate(vt[$],vt[me],vt[Z],vt[Me]),M=$,S=me,w=Z,I=Me),($e.equals(_)===!1||$t!==C)&&(n.blendColor($e.r,$e.g,$e.b,$t),_.copy($e),C=$t),m=R,O=!1}function tt(R,ie){R.side===fi?Oe(n.CULL_FACE):ee(n.CULL_FACE);let $=R.side===Pn;ie&&($=!$),Kt($),R.blending===so&&R.transparent===!1?Ot(tr):Ot(R.blending,R.blendEquation,R.blendSrc,R.blendDst,R.blendEquationAlpha,R.blendSrcAlpha,R.blendDstAlpha,R.blendColor,R.blendAlpha,R.premultipliedAlpha),o.setFunc(R.depthFunc),o.setTest(R.depthTest),o.setMask(R.depthWrite),s.setMask(R.colorWrite);let me=R.stencilWrite;a.setTest(me),me&&(a.setMask(R.stencilWriteMask),a.setFunc(R.stencilFunc,R.stencilRef,R.stencilFuncMask),a.setOp(R.stencilFail,R.stencilZFail,R.stencilZPass)),N(R.polygonOffset,R.polygonOffsetFactor,R.polygonOffsetUnits),R.alphaToCoverage===!0?ee(n.SAMPLE_ALPHA_TO_COVERAGE):Oe(n.SAMPLE_ALPHA_TO_COVERAGE)}function Kt(R){D!==R&&(R?n.frontFace(n.CW):n.frontFace(n.CCW),D=R)}function kt(R){R!==SS?(ee(n.CULL_FACE),R!==L&&(R===vy?n.cullFace(n.BACK):R===MS?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Oe(n.CULL_FACE),L=R}function jn(R){R!==G&&(U&&n.lineWidth(R),G=R)}function N(R,ie,$){R?(ee(n.POLYGON_OFFSET_FILL),(W!==ie||P!==$)&&(W=ie,P=$,o.getReversed()&&(ie=-ie),n.polygonOffset(ie,$))):Oe(n.POLYGON_OFFSET_FILL)}function Jt(R){R?ee(n.SCISSOR_TEST):Oe(n.SCISSOR_TEST)}function ot(R){R===void 0&&(R=n.TEXTURE0+H-1),ce!==R&&(n.activeTexture(R),ce=R)}function At(R,ie,$){$===void 0&&(ce===null?$=n.TEXTURE0+H-1:$=ce);let me=xe[$];me===void 0&&(me={type:void 0,texture:void 0},xe[$]=me),(me.type!==R||me.texture!==ie)&&(ce!==$&&(n.activeTexture($),ce=$),n.bindTexture(R,ie||le[R]),me.type=R,me.texture=ie)}function ue(){let R=xe[ce];R!==void 0&&R.type!==void 0&&(n.bindTexture(R.type,null),R.type=void 0,R.texture=void 0)}function Vt(){try{n.compressedTexImage2D(...arguments)}catch(R){Pe("WebGLState:",R)}}function E(){try{n.compressedTexImage3D(...arguments)}catch(R){Pe("WebGLState:",R)}}function v(){try{n.texSubImage2D(...arguments)}catch(R){Pe("WebGLState:",R)}}function k(){try{n.texSubImage3D(...arguments)}catch(R){Pe("WebGLState:",R)}}function q(){try{n.compressedTexSubImage2D(...arguments)}catch(R){Pe("WebGLState:",R)}}function K(){try{n.compressedTexSubImage3D(...arguments)}catch(R){Pe("WebGLState:",R)}}function te(){try{n.texStorage2D(...arguments)}catch(R){Pe("WebGLState:",R)}}function oe(){try{n.texStorage3D(...arguments)}catch(R){Pe("WebGLState:",R)}}function j(){try{n.texImage2D(...arguments)}catch(R){Pe("WebGLState:",R)}}function Y(){try{n.texImage3D(...arguments)}catch(R){Pe("WebGLState:",R)}}function he(R){return d[R]!==void 0?d[R]:n.getParameter(R)}function ye(R,ie){d[R]!==ie&&(n.pixelStorei(R,ie),d[R]=ie)}function re(R){Ke.equals(R)===!1&&(n.scissor(R.x,R.y,R.z,R.w),Ke.copy(R))}function ne(R){Fe.equals(R)===!1&&(n.viewport(R.x,R.y,R.z,R.w),Fe.copy(R))}function He(R,ie){let $=l.get(ie);$===void 0&&($=new WeakMap,l.set(ie,$));let me=$.get(R);me===void 0&&(me=n.getUniformBlockIndex(ie,R.name),$.set(R,me))}function Xe(R,ie){let me=l.get(ie).get(R);c.get(ie)!==me&&(n.uniformBlockBinding(ie,me,R.__bindingPointIndex),c.set(ie,me))}function dt(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),n.pixelStorei(n.PACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,!1),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,!1),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,n.BROWSER_DEFAULT_WEBGL),n.pixelStorei(n.PACK_ROW_LENGTH,0),n.pixelStorei(n.PACK_SKIP_PIXELS,0),n.pixelStorei(n.PACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_ROW_LENGTH,0),n.pixelStorei(n.UNPACK_IMAGE_HEIGHT,0),n.pixelStorei(n.UNPACK_SKIP_PIXELS,0),n.pixelStorei(n.UNPACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_SKIP_IMAGES,0),u={},d={},ce=null,xe={},f={},h=new WeakMap,g=[],y=null,p=!1,m=null,b=null,M=null,S=null,A=null,w=null,I=null,_=new Ve(0,0,0),C=0,O=!1,D=null,L=null,G=null,W=null,P=null,Ke.set(0,0,n.canvas.width,n.canvas.height),Fe.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ee,disable:Oe,bindFramebuffer:We,drawBuffers:Le,useProgram:Lt,setBlending:Ot,setMaterial:tt,setFlipSided:Kt,setCullFace:kt,setLineWidth:jn,setPolygonOffset:N,setScissorTest:Jt,activeTexture:ot,bindTexture:At,unbindTexture:ue,compressedTexImage2D:Vt,compressedTexImage3D:E,texImage2D:j,texImage3D:Y,pixelStorei:ye,getParameter:he,updateUBOMapping:He,uniformBlockBinding:Xe,texStorage2D:te,texStorage3D:oe,texSubImage2D:v,texSubImage3D:k,compressedTexSubImage2D:q,compressedTexSubImage3D:K,scissor:re,viewport:ne,reset:dt}}function e2(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new Te,u=new WeakMap,d=new Set,f,h=new WeakMap,g=!1;try{g=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch(E){}function y(E,v){return g?new OffscreenCanvas(E,v):Qc("canvas")}function p(E,v,k){let q=1,K=Vt(E);if((K.width>k||K.height>k)&&(q=k/Math.max(K.width,K.height)),q<1)if(typeof HTMLImageElement<"u"&&E instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&E instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&E instanceof ImageBitmap||typeof VideoFrame<"u"&&E instanceof VideoFrame){let te=Math.floor(q*K.width),oe=Math.floor(q*K.height);f===void 0&&(f=y(te,oe));let j=v?y(te,oe):f;return j.width=te,j.height=oe,j.getContext("2d").drawImage(E,0,0,te,oe),Re("WebGLRenderer: Texture has been resized from ("+K.width+"x"+K.height+") to ("+te+"x"+oe+")."),j}else return"data"in E&&Re("WebGLRenderer: Image in DataTexture is too big ("+K.width+"x"+K.height+")."),E;return E}function m(E){return E.generateMipmaps}function b(E){n.generateMipmap(E)}function M(E){return E.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:E.isWebGL3DRenderTarget?n.TEXTURE_3D:E.isWebGLArrayRenderTarget||E.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function S(E,v,k,q,K,te=!1){if(E!==null){if(n[E]!==void 0)return n[E];Re("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+E+"'")}let oe;q&&(oe=e.get("EXT_texture_norm16"),oe||Re("WebGLRenderer: Unable to use normalized textures without EXT_texture_norm16 extension"));let j=v;if(v===n.RED&&(k===n.FLOAT&&(j=n.R32F),k===n.HALF_FLOAT&&(j=n.R16F),k===n.UNSIGNED_BYTE&&(j=n.R8),k===n.UNSIGNED_SHORT&&oe&&(j=oe.R16_EXT),k===n.SHORT&&oe&&(j=oe.R16_SNORM_EXT)),v===n.RED_INTEGER&&(k===n.UNSIGNED_BYTE&&(j=n.R8UI),k===n.UNSIGNED_SHORT&&(j=n.R16UI),k===n.UNSIGNED_INT&&(j=n.R32UI),k===n.BYTE&&(j=n.R8I),k===n.SHORT&&(j=n.R16I),k===n.INT&&(j=n.R32I)),v===n.RG&&(k===n.FLOAT&&(j=n.RG32F),k===n.HALF_FLOAT&&(j=n.RG16F),k===n.UNSIGNED_BYTE&&(j=n.RG8),k===n.UNSIGNED_SHORT&&oe&&(j=oe.RG16_EXT),k===n.SHORT&&oe&&(j=oe.RG16_SNORM_EXT)),v===n.RG_INTEGER&&(k===n.UNSIGNED_BYTE&&(j=n.RG8UI),k===n.UNSIGNED_SHORT&&(j=n.RG16UI),k===n.UNSIGNED_INT&&(j=n.RG32UI),k===n.BYTE&&(j=n.RG8I),k===n.SHORT&&(j=n.RG16I),k===n.INT&&(j=n.RG32I)),v===n.RGB_INTEGER&&(k===n.UNSIGNED_BYTE&&(j=n.RGB8UI),k===n.UNSIGNED_SHORT&&(j=n.RGB16UI),k===n.UNSIGNED_INT&&(j=n.RGB32UI),k===n.BYTE&&(j=n.RGB8I),k===n.SHORT&&(j=n.RGB16I),k===n.INT&&(j=n.RGB32I)),v===n.RGBA_INTEGER&&(k===n.UNSIGNED_BYTE&&(j=n.RGBA8UI),k===n.UNSIGNED_SHORT&&(j=n.RGBA16UI),k===n.UNSIGNED_INT&&(j=n.RGBA32UI),k===n.BYTE&&(j=n.RGBA8I),k===n.SHORT&&(j=n.RGBA16I),k===n.INT&&(j=n.RGBA32I)),v===n.RGB&&(k===n.UNSIGNED_SHORT&&oe&&(j=oe.RGB16_EXT),k===n.SHORT&&oe&&(j=oe.RGB16_SNORM_EXT),k===n.UNSIGNED_INT_5_9_9_9_REV&&(j=n.RGB9_E5),k===n.UNSIGNED_INT_10F_11F_11F_REV&&(j=n.R11F_G11F_B10F)),v===n.RGBA){let Y=te?Jc:at.getTransfer(K);k===n.FLOAT&&(j=n.RGBA32F),k===n.HALF_FLOAT&&(j=n.RGBA16F),k===n.UNSIGNED_BYTE&&(j=Y===pt?n.SRGB8_ALPHA8:n.RGBA8),k===n.UNSIGNED_SHORT&&oe&&(j=oe.RGBA16_EXT),k===n.SHORT&&oe&&(j=oe.RGBA16_SNORM_EXT),k===n.UNSIGNED_SHORT_4_4_4_4&&(j=n.RGBA4),k===n.UNSIGNED_SHORT_5_5_5_1&&(j=n.RGB5_A1)}return(j===n.R16F||j===n.R32F||j===n.RG16F||j===n.RG32F||j===n.RGBA16F||j===n.RGBA32F)&&e.get("EXT_color_buffer_float"),j}function A(E,v){let k;return E?v===null||v===Ii||v===Va?k=n.DEPTH24_STENCIL8:v===Ri?k=n.DEPTH32F_STENCIL8:v===Ba&&(k=n.DEPTH24_STENCIL8,Re("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===Ii||v===Va?k=n.DEPTH_COMPONENT24:v===Ri?k=n.DEPTH_COMPONENT32F:v===Ba&&(k=n.DEPTH_COMPONENT16),k}function w(E,v){return m(E)===!0||E.isFramebufferTexture&&E.minFilter!==an&&E.minFilter!==hn?Math.log2(Math.max(v.width,v.height))+1:E.mipmaps!==void 0&&E.mipmaps.length>0?E.mipmaps.length:E.isCompressedTexture&&Array.isArray(E.image)?v.mipmaps.length:1}function I(E){let v=E.target;v.removeEventListener("dispose",I),C(v),v.isVideoTexture&&u.delete(v),v.isHTMLTexture&&d.delete(v)}function _(E){let v=E.target;v.removeEventListener("dispose",_),D(v)}function C(E){let v=i.get(E);if(v.__webglInit===void 0)return;let k=E.source,q=h.get(k);if(q){let K=q[v.__cacheKey];K.usedTimes--,K.usedTimes===0&&O(E),Object.keys(q).length===0&&h.delete(k)}i.remove(E)}function O(E){let v=i.get(E);n.deleteTexture(v.__webglTexture);let k=E.source,q=h.get(k);delete q[v.__cacheKey],o.memory.textures--}function D(E){let v=i.get(E);if(E.depthTexture&&(E.depthTexture.dispose(),i.remove(E.depthTexture)),E.isWebGLCubeRenderTarget)for(let q=0;q<6;q++){if(Array.isArray(v.__webglFramebuffer[q]))for(let K=0;K<v.__webglFramebuffer[q].length;K++)n.deleteFramebuffer(v.__webglFramebuffer[q][K]);else n.deleteFramebuffer(v.__webglFramebuffer[q]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[q])}else{if(Array.isArray(v.__webglFramebuffer))for(let q=0;q<v.__webglFramebuffer.length;q++)n.deleteFramebuffer(v.__webglFramebuffer[q]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let q=0;q<v.__webglColorRenderbuffer.length;q++)v.__webglColorRenderbuffer[q]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[q]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let k=E.textures;for(let q=0,K=k.length;q<K;q++){let te=i.get(k[q]);te.__webglTexture&&(n.deleteTexture(te.__webglTexture),o.memory.textures--),i.remove(k[q])}i.remove(E)}let L=0;function G(){L=0}function W(){return L}function P(E){L=E}function H(){let E=L;return E>=r.maxTextures&&Re("WebGLTextures: Trying to use "+E+" texture units while this GPU supports only "+r.maxTextures),L+=1,E}function U(E){let v=[];return v.push(E.wrapS),v.push(E.wrapT),v.push(E.wrapR||0),v.push(E.magFilter),v.push(E.minFilter),v.push(E.anisotropy),v.push(E.internalFormat),v.push(E.format),v.push(E.type),v.push(E.generateMipmaps),v.push(E.premultiplyAlpha),v.push(E.flipY),v.push(E.unpackAlignment),v.push(E.colorSpace),v.join()}function J(E,v){let k=i.get(E);if(E.isVideoTexture&&At(E),E.isRenderTargetTexture===!1&&E.isExternalTexture!==!0&&E.version>0&&k.__version!==E.version){let q=E.image;if(q===null)Re("WebGLRenderer: Texture marked for update but no image data found.");else if(q.complete===!1)Re("WebGLRenderer: Texture marked for update but image is incomplete");else{Oe(k,E,v);return}}else E.isExternalTexture&&(k.__webglTexture=E.sourceTexture?E.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,k.__webglTexture,n.TEXTURE0+v)}function Q(E,v){let k=i.get(E);if(E.isRenderTargetTexture===!1&&E.version>0&&k.__version!==E.version){Oe(k,E,v);return}else E.isExternalTexture&&(k.__webglTexture=E.sourceTexture?E.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,k.__webglTexture,n.TEXTURE0+v)}function ce(E,v){let k=i.get(E);if(E.isRenderTargetTexture===!1&&E.version>0&&k.__version!==E.version){Oe(k,E,v);return}t.bindTexture(n.TEXTURE_3D,k.__webglTexture,n.TEXTURE0+v)}function xe(E,v){let k=i.get(E);if(E.isCubeDepthTexture!==!0&&E.version>0&&k.__version!==E.version){We(k,E,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,k.__webglTexture,n.TEXTURE0+v)}let Se={[Vf]:n.REPEAT,[Ji]:n.CLAMP_TO_EDGE,[Hf]:n.MIRRORED_REPEAT},rt={[an]:n.NEAREST,[$S]:n.NEAREST_MIPMAP_NEAREST,[wl]:n.NEAREST_MIPMAP_LINEAR,[hn]:n.LINEAR,[gh]:n.LINEAR_MIPMAP_NEAREST,[_s]:n.LINEAR_MIPMAP_LINEAR},Ke={[YS]:n.NEVER,[eM]:n.ALWAYS,[ZS]:n.LESS,[ep]:n.LEQUAL,[KS]:n.EQUAL,[tp]:n.GEQUAL,[JS]:n.GREATER,[QS]:n.NOTEQUAL};function Fe(E,v){if(v.type===Ri&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===hn||v.magFilter===gh||v.magFilter===wl||v.magFilter===_s||v.minFilter===hn||v.minFilter===gh||v.minFilter===wl||v.minFilter===_s)&&Re("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(E,n.TEXTURE_WRAP_S,Se[v.wrapS]),n.texParameteri(E,n.TEXTURE_WRAP_T,Se[v.wrapT]),(E===n.TEXTURE_3D||E===n.TEXTURE_2D_ARRAY)&&n.texParameteri(E,n.TEXTURE_WRAP_R,Se[v.wrapR]),n.texParameteri(E,n.TEXTURE_MAG_FILTER,rt[v.magFilter]),n.texParameteri(E,n.TEXTURE_MIN_FILTER,rt[v.minFilter]),v.compareFunction&&(n.texParameteri(E,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(E,n.TEXTURE_COMPARE_FUNC,Ke[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===an||v.minFilter!==wl&&v.minFilter!==_s||v.type===Ri&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let k=e.get("EXT_texture_filter_anisotropic");n.texParameterf(E,k.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function X(E,v){let k=!1;E.__webglInit===void 0&&(E.__webglInit=!0,v.addEventListener("dispose",I));let q=v.source,K=h.get(q);K===void 0&&(K={},h.set(q,K));let te=U(v);if(te!==E.__cacheKey){K[te]===void 0&&(K[te]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,k=!0),K[te].usedTimes++;let oe=K[E.__cacheKey];oe!==void 0&&(K[E.__cacheKey].usedTimes--,oe.usedTimes===0&&O(v)),E.__cacheKey=te,E.__webglTexture=K[te].texture}return k}function le(E,v,k){return Math.floor(Math.floor(E/k)/v)}function ee(E,v,k,q){let te=E.updateRanges;if(te.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,k,q,v.data);else{te.sort((ye,re)=>ye.start-re.start);let oe=0;for(let ye=1;ye<te.length;ye++){let re=te[oe],ne=te[ye],He=re.start+re.count,Xe=le(ne.start,v.width,4),dt=le(re.start,v.width,4);ne.start<=He+1&&Xe===dt&&le(ne.start+ne.count-1,v.width,4)===Xe?re.count=Math.max(re.count,ne.start+ne.count-re.start):(++oe,te[oe]=ne)}te.length=oe+1;let j=t.getParameter(n.UNPACK_ROW_LENGTH),Y=t.getParameter(n.UNPACK_SKIP_PIXELS),he=t.getParameter(n.UNPACK_SKIP_ROWS);t.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let ye=0,re=te.length;ye<re;ye++){let ne=te[ye],He=Math.floor(ne.start/4),Xe=Math.ceil(ne.count/4),dt=He%v.width,R=Math.floor(He/v.width),ie=Xe,$=1;t.pixelStorei(n.UNPACK_SKIP_PIXELS,dt),t.pixelStorei(n.UNPACK_SKIP_ROWS,R),t.texSubImage2D(n.TEXTURE_2D,0,dt,R,ie,$,k,q,v.data)}E.clearUpdateRanges(),t.pixelStorei(n.UNPACK_ROW_LENGTH,j),t.pixelStorei(n.UNPACK_SKIP_PIXELS,Y),t.pixelStorei(n.UNPACK_SKIP_ROWS,he)}}function Oe(E,v,k){let q=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&(q=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&(q=n.TEXTURE_3D);let K=X(E,v),te=v.source;t.bindTexture(q,E.__webglTexture,n.TEXTURE0+k);let oe=i.get(te);if(te.version!==oe.__version||K===!0){if(t.activeTexture(n.TEXTURE0+k),(typeof ImageBitmap<"u"&&v.image instanceof ImageBitmap)===!1){let $=at.getPrimaries(at.workingColorSpace),me=v.colorSpace===Pr?null:at.getPrimaries(v.colorSpace),se=v.colorSpace===Pr||$===me?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,se)}t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment);let Y=p(v.image,!1,r.maxTextureSize);Y=ue(v,Y);let he=s.convert(v.format,v.colorSpace),ye=s.convert(v.type),re=S(v.internalFormat,he,ye,v.normalized,v.colorSpace,v.isVideoTexture);Fe(q,v);let ne,He=v.mipmaps,Xe=v.isVideoTexture!==!0,dt=oe.__version===void 0||K===!0,R=te.dataReady,ie=w(v,Y);if(v.isDepthTexture)re=A(v.format===xs,v.type),dt&&(Xe?t.texStorage2D(n.TEXTURE_2D,1,re,Y.width,Y.height):t.texImage2D(n.TEXTURE_2D,0,re,Y.width,Y.height,0,he,ye,null));else if(v.isDataTexture)if(He.length>0){Xe&&dt&&t.texStorage2D(n.TEXTURE_2D,ie,re,He[0].width,He[0].height);for(let $=0,me=He.length;$<me;$++)ne=He[$],Xe?R&&t.texSubImage2D(n.TEXTURE_2D,$,0,0,ne.width,ne.height,he,ye,ne.data):t.texImage2D(n.TEXTURE_2D,$,re,ne.width,ne.height,0,he,ye,ne.data);v.generateMipmaps=!1}else Xe?(dt&&t.texStorage2D(n.TEXTURE_2D,ie,re,Y.width,Y.height),R&&ee(v,Y,he,ye)):t.texImage2D(n.TEXTURE_2D,0,re,Y.width,Y.height,0,he,ye,Y.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){Xe&&dt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ie,re,He[0].width,He[0].height,Y.depth);for(let $=0,me=He.length;$<me;$++)if(ne=He[$],v.format!==hi)if(he!==null)if(Xe){if(R)if(v.layerUpdates.size>0){let se=Hy(ne.width,ne.height,v.format,v.type);for(let Z of v.layerUpdates){let Me=ne.data.subarray(Z*se/ne.data.BYTES_PER_ELEMENT,(Z+1)*se/ne.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,$,0,0,Z,ne.width,ne.height,1,he,Me)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,$,0,0,0,ne.width,ne.height,Y.depth,he,ne.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,$,re,ne.width,ne.height,Y.depth,0,ne.data,0,0);else Re("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Xe?R&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,$,0,0,0,ne.width,ne.height,Y.depth,he,ye,ne.data):t.texImage3D(n.TEXTURE_2D_ARRAY,$,re,ne.width,ne.height,Y.depth,0,he,ye,ne.data)}else{Xe&&dt&&t.texStorage2D(n.TEXTURE_2D,ie,re,He[0].width,He[0].height);for(let $=0,me=He.length;$<me;$++)ne=He[$],v.format!==hi?he!==null?Xe?R&&t.compressedTexSubImage2D(n.TEXTURE_2D,$,0,0,ne.width,ne.height,he,ne.data):t.compressedTexImage2D(n.TEXTURE_2D,$,re,ne.width,ne.height,0,ne.data):Re("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Xe?R&&t.texSubImage2D(n.TEXTURE_2D,$,0,0,ne.width,ne.height,he,ye,ne.data):t.texImage2D(n.TEXTURE_2D,$,re,ne.width,ne.height,0,he,ye,ne.data)}else if(v.isDataArrayTexture)if(Xe){if(dt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ie,re,Y.width,Y.height,Y.depth),R)if(v.layerUpdates.size>0){let $=Hy(Y.width,Y.height,v.format,v.type);for(let me of v.layerUpdates){let se=Y.data.subarray(me*$/Y.data.BYTES_PER_ELEMENT,(me+1)*$/Y.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,me,Y.width,Y.height,1,he,ye,se)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,Y.width,Y.height,Y.depth,he,ye,Y.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,re,Y.width,Y.height,Y.depth,0,he,ye,Y.data);else if(v.isData3DTexture)Xe?(dt&&t.texStorage3D(n.TEXTURE_3D,ie,re,Y.width,Y.height,Y.depth),R&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,Y.width,Y.height,Y.depth,he,ye,Y.data)):t.texImage3D(n.TEXTURE_3D,0,re,Y.width,Y.height,Y.depth,0,he,ye,Y.data);else if(v.isFramebufferTexture){if(dt)if(Xe)t.texStorage2D(n.TEXTURE_2D,ie,re,Y.width,Y.height);else{let $=Y.width,me=Y.height;for(let se=0;se<ie;se++)t.texImage2D(n.TEXTURE_2D,se,re,$,me,0,he,ye,null),$>>=1,me>>=1}}else if(v.isHTMLTexture){if("texElementImage2D"in n){let $=n.canvas;if($.hasAttribute("layoutsubtree")||$.setAttribute("layoutsubtree","true"),Y.parentNode!==$){$.appendChild(Y),d.add(v),$.onpaint=$e=>{let $t=$e.changedElements;for(let yt of d)$t.includes(yt.image)&&(yt.needsUpdate=!0)},$.requestPaint();return}let me=0,se=n.RGBA,Z=n.RGBA,Me=n.UNSIGNED_BYTE;n.texElementImage2D(n.TEXTURE_2D,me,se,Z,Me,Y),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_MIN_FILTER,n.LINEAR),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_S,n.CLAMP_TO_EDGE),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_T,n.CLAMP_TO_EDGE)}}else if(He.length>0){if(Xe&&dt){let $=Vt(He[0]);t.texStorage2D(n.TEXTURE_2D,ie,re,$.width,$.height)}for(let $=0,me=He.length;$<me;$++)ne=He[$],Xe?R&&t.texSubImage2D(n.TEXTURE_2D,$,0,0,he,ye,ne):t.texImage2D(n.TEXTURE_2D,$,re,he,ye,ne);v.generateMipmaps=!1}else if(Xe){if(dt){let $=Vt(Y);t.texStorage2D(n.TEXTURE_2D,ie,re,$.width,$.height)}R&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,he,ye,Y)}else t.texImage2D(n.TEXTURE_2D,0,re,he,ye,Y);m(v)&&b(q),oe.__version=te.version,v.onUpdate&&v.onUpdate(v)}E.__version=v.version}function We(E,v,k){if(v.image.length!==6)return;let q=X(E,v),K=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,E.__webglTexture,n.TEXTURE0+k);let te=i.get(K);if(K.version!==te.__version||q===!0){t.activeTexture(n.TEXTURE0+k);let oe=at.getPrimaries(at.workingColorSpace),j=v.colorSpace===Pr?null:at.getPrimaries(v.colorSpace),Y=v.colorSpace===Pr||oe===j?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Y);let he=v.isCompressedTexture||v.image[0].isCompressedTexture,ye=v.image[0]&&v.image[0].isDataTexture,re=[];for(let Z=0;Z<6;Z++)!he&&!ye?re[Z]=p(v.image[Z],!0,r.maxCubemapSize):re[Z]=ye?v.image[Z].image:v.image[Z],re[Z]=ue(v,re[Z]);let ne=re[0],He=s.convert(v.format,v.colorSpace),Xe=s.convert(v.type),dt=S(v.internalFormat,He,Xe,v.normalized,v.colorSpace),R=v.isVideoTexture!==!0,ie=te.__version===void 0||q===!0,$=K.dataReady,me=w(v,ne);Fe(n.TEXTURE_CUBE_MAP,v);let se;if(he){R&&ie&&t.texStorage2D(n.TEXTURE_CUBE_MAP,me,dt,ne.width,ne.height);for(let Z=0;Z<6;Z++){se=re[Z].mipmaps;for(let Me=0;Me<se.length;Me++){let $e=se[Me];v.format!==hi?He!==null?R?$&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Me,0,0,$e.width,$e.height,He,$e.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Me,dt,$e.width,$e.height,0,$e.data):Re("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):R?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Me,0,0,$e.width,$e.height,He,Xe,$e.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Me,dt,$e.width,$e.height,0,He,Xe,$e.data)}}}else{if(se=v.mipmaps,R&&ie){se.length>0&&me++;let Z=Vt(re[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,me,dt,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(ye){R?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,re[Z].width,re[Z].height,He,Xe,re[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,dt,re[Z].width,re[Z].height,0,He,Xe,re[Z].data);for(let Me=0;Me<se.length;Me++){let $t=se[Me].image[Z].image;R?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Me+1,0,0,$t.width,$t.height,He,Xe,$t.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Me+1,dt,$t.width,$t.height,0,He,Xe,$t.data)}}else{R?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,He,Xe,re[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,dt,He,Xe,re[Z]);for(let Me=0;Me<se.length;Me++){let $e=se[Me];R?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Me+1,0,0,He,Xe,$e.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Me+1,dt,He,Xe,$e.image[Z])}}}m(v)&&b(n.TEXTURE_CUBE_MAP),te.__version=K.version,v.onUpdate&&v.onUpdate(v)}E.__version=v.version}function Le(E,v,k,q,K,te){let oe=s.convert(k.format,k.colorSpace),j=s.convert(k.type),Y=S(k.internalFormat,oe,j,k.normalized,k.colorSpace),he=i.get(v),ye=i.get(k);if(ye.__renderTarget=v,!he.__hasExternalTextures){let re=Math.max(1,v.width>>te),ne=Math.max(1,v.height>>te);K===n.TEXTURE_3D||K===n.TEXTURE_2D_ARRAY?t.texImage3D(K,te,Y,re,ne,v.depth,0,oe,j,null):t.texImage2D(K,te,Y,re,ne,0,oe,j,null)}t.bindFramebuffer(n.FRAMEBUFFER,E),ot(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,q,K,ye.__webglTexture,0,Jt(v)):(K===n.TEXTURE_2D||K>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&K<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,q,K,ye.__webglTexture,te),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Lt(E,v,k){if(n.bindRenderbuffer(n.RENDERBUFFER,E),v.depthBuffer){let q=v.depthTexture,K=q&&q.isDepthTexture?q.type:null,te=A(v.stencilBuffer,K),oe=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;ot(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Jt(v),te,v.width,v.height):k?n.renderbufferStorageMultisample(n.RENDERBUFFER,Jt(v),te,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,te,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,oe,n.RENDERBUFFER,E)}else{let q=v.textures;for(let K=0;K<q.length;K++){let te=q[K],oe=s.convert(te.format,te.colorSpace),j=s.convert(te.type),Y=S(te.internalFormat,oe,j,te.normalized,te.colorSpace);ot(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Jt(v),Y,v.width,v.height):k?n.renderbufferStorageMultisample(n.RENDERBUFFER,Jt(v),Y,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,Y,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function st(E,v,k){let q=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,E),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let K=i.get(v.depthTexture);if(K.__renderTarget=v,(!K.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),q){if(K.__webglInit===void 0&&(K.__webglInit=!0,v.depthTexture.addEventListener("dispose",I)),K.__webglTexture===void 0){K.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,K.__webglTexture),Fe(n.TEXTURE_CUBE_MAP,v.depthTexture);let he=s.convert(v.depthTexture.format),ye=s.convert(v.depthTexture.type),re;v.depthTexture.format===Qi?re=n.DEPTH_COMPONENT24:v.depthTexture.format===xs&&(re=n.DEPTH24_STENCIL8);for(let ne=0;ne<6;ne++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+ne,0,re,v.width,v.height,0,he,ye,null)}}else J(v.depthTexture,0);let te=K.__webglTexture,oe=Jt(v),j=q?n.TEXTURE_CUBE_MAP_POSITIVE_X+k:n.TEXTURE_2D,Y=v.depthTexture.format===xs?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===Qi)ot(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Y,j,te,0,oe):n.framebufferTexture2D(n.FRAMEBUFFER,Y,j,te,0);else if(v.depthTexture.format===xs)ot(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Y,j,te,0,oe):n.framebufferTexture2D(n.FRAMEBUFFER,Y,j,te,0);else throw new Error("Unknown depthTexture format")}function vt(E){let v=i.get(E),k=E.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==E.depthTexture){let q=E.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),q){let K=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,q.removeEventListener("dispose",K)};q.addEventListener("dispose",K),v.__depthDisposeCallback=K}v.__boundDepthTexture=q}if(E.depthTexture&&!v.__autoAllocateDepthBuffer)if(k)for(let q=0;q<6;q++)st(v.__webglFramebuffer[q],E,q);else{let q=E.texture.mipmaps;q&&q.length>0?st(v.__webglFramebuffer[0],E,0):st(v.__webglFramebuffer,E,0)}else if(k){v.__webglDepthbuffer=[];for(let q=0;q<6;q++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[q]),v.__webglDepthbuffer[q]===void 0)v.__webglDepthbuffer[q]=n.createRenderbuffer(),Lt(v.__webglDepthbuffer[q],E,!1);else{let K=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,te=v.__webglDepthbuffer[q];n.bindRenderbuffer(n.RENDERBUFFER,te),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,te)}}else{let q=E.texture.mipmaps;if(q&&q.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Lt(v.__webglDepthbuffer,E,!1);else{let K=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,te=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,te),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,te)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ot(E,v,k){let q=i.get(E);v!==void 0&&Le(q.__webglFramebuffer,E,E.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),k!==void 0&&vt(E)}function tt(E){let v=E.texture,k=i.get(E),q=i.get(v);E.addEventListener("dispose",_);let K=E.textures,te=E.isWebGLCubeRenderTarget===!0,oe=K.length>1;if(oe||(q.__webglTexture===void 0&&(q.__webglTexture=n.createTexture()),q.__version=v.version,o.memory.textures++),te){k.__webglFramebuffer=[];for(let j=0;j<6;j++)if(v.mipmaps&&v.mipmaps.length>0){k.__webglFramebuffer[j]=[];for(let Y=0;Y<v.mipmaps.length;Y++)k.__webglFramebuffer[j][Y]=n.createFramebuffer()}else k.__webglFramebuffer[j]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){k.__webglFramebuffer=[];for(let j=0;j<v.mipmaps.length;j++)k.__webglFramebuffer[j]=n.createFramebuffer()}else k.__webglFramebuffer=n.createFramebuffer();if(oe)for(let j=0,Y=K.length;j<Y;j++){let he=i.get(K[j]);he.__webglTexture===void 0&&(he.__webglTexture=n.createTexture(),o.memory.textures++)}if(E.samples>0&&ot(E)===!1){k.__webglMultisampledFramebuffer=n.createFramebuffer(),k.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,k.__webglMultisampledFramebuffer);for(let j=0;j<K.length;j++){let Y=K[j];k.__webglColorRenderbuffer[j]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,k.__webglColorRenderbuffer[j]);let he=s.convert(Y.format,Y.colorSpace),ye=s.convert(Y.type),re=S(Y.internalFormat,he,ye,Y.normalized,Y.colorSpace,E.isXRRenderTarget===!0),ne=Jt(E);n.renderbufferStorageMultisample(n.RENDERBUFFER,ne,re,E.width,E.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+j,n.RENDERBUFFER,k.__webglColorRenderbuffer[j])}n.bindRenderbuffer(n.RENDERBUFFER,null),E.depthBuffer&&(k.__webglDepthRenderbuffer=n.createRenderbuffer(),Lt(k.__webglDepthRenderbuffer,E,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(te){t.bindTexture(n.TEXTURE_CUBE_MAP,q.__webglTexture),Fe(n.TEXTURE_CUBE_MAP,v);for(let j=0;j<6;j++)if(v.mipmaps&&v.mipmaps.length>0)for(let Y=0;Y<v.mipmaps.length;Y++)Le(k.__webglFramebuffer[j][Y],E,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+j,Y);else Le(k.__webglFramebuffer[j],E,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0);m(v)&&b(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(oe){for(let j=0,Y=K.length;j<Y;j++){let he=K[j],ye=i.get(he),re=n.TEXTURE_2D;(E.isWebGL3DRenderTarget||E.isWebGLArrayRenderTarget)&&(re=E.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(re,ye.__webglTexture),Fe(re,he),Le(k.__webglFramebuffer,E,he,n.COLOR_ATTACHMENT0+j,re,0),m(he)&&b(re)}t.unbindTexture()}else{let j=n.TEXTURE_2D;if((E.isWebGL3DRenderTarget||E.isWebGLArrayRenderTarget)&&(j=E.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(j,q.__webglTexture),Fe(j,v),v.mipmaps&&v.mipmaps.length>0)for(let Y=0;Y<v.mipmaps.length;Y++)Le(k.__webglFramebuffer[Y],E,v,n.COLOR_ATTACHMENT0,j,Y);else Le(k.__webglFramebuffer,E,v,n.COLOR_ATTACHMENT0,j,0);m(v)&&b(j),t.unbindTexture()}E.depthBuffer&&vt(E)}function Kt(E){let v=E.textures;for(let k=0,q=v.length;k<q;k++){let K=v[k];if(m(K)){let te=M(E),oe=i.get(K).__webglTexture;t.bindTexture(te,oe),b(te),t.unbindTexture()}}}let kt=[],jn=[];function N(E){if(E.samples>0){if(ot(E)===!1){let v=E.textures,k=E.width,q=E.height,K=n.COLOR_BUFFER_BIT,te=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,oe=i.get(E),j=v.length>1;if(j)for(let he=0;he<v.length;he++)t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,oe.__webglMultisampledFramebuffer);let Y=E.texture.mipmaps;Y&&Y.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglFramebuffer);for(let he=0;he<v.length;he++){if(E.resolveDepthBuffer&&(E.depthBuffer&&(K|=n.DEPTH_BUFFER_BIT),E.stencilBuffer&&E.resolveStencilBuffer&&(K|=n.STENCIL_BUFFER_BIT)),j){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,oe.__webglColorRenderbuffer[he]);let ye=i.get(v[he]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,ye,0)}n.blitFramebuffer(0,0,k,q,0,0,k,q,K,n.NEAREST),c===!0&&(kt.length=0,jn.length=0,kt.push(n.COLOR_ATTACHMENT0+he),E.depthBuffer&&E.resolveDepthBuffer===!1&&(kt.push(te),jn.push(te),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,jn)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,kt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),j)for(let he=0;he<v.length;he++){t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,oe.__webglColorRenderbuffer[he]);let ye=i.get(v[he]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,ye,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglMultisampledFramebuffer)}else if(E.depthBuffer&&E.resolveDepthBuffer===!1&&c){let v=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function Jt(E){return Math.min(r.maxSamples,E.samples)}function ot(E){let v=i.get(E);return E.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function At(E){let v=o.render.frame;u.get(E)!==v&&(u.set(E,v),E.update())}function ue(E,v){let k=E.colorSpace,q=E.format,K=E.type;return E.isCompressedTexture===!0||E.isVideoTexture===!0||k!==Kc&&k!==Pr&&(at.getTransfer(k)===pt?(q!==hi||K!==zn)&&Re("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Pe("WebGLTextures: Unsupported texture color space:",k)),v}function Vt(E){return typeof HTMLImageElement<"u"&&E instanceof HTMLImageElement?(l.width=E.naturalWidth||E.width,l.height=E.naturalHeight||E.height):typeof VideoFrame<"u"&&E instanceof VideoFrame?(l.width=E.displayWidth,l.height=E.displayHeight):(l.width=E.width,l.height=E.height),l}this.allocateTextureUnit=H,this.resetTextureUnits=G,this.getTextureUnits=W,this.setTextureUnits=P,this.setTexture2D=J,this.setTexture2DArray=Q,this.setTexture3D=ce,this.setTextureCube=xe,this.rebindTextures=Ot,this.setupRenderTarget=tt,this.updateRenderTargetMipmap=Kt,this.updateMultisampleRenderTarget=N,this.setupDepthRenderbuffer=vt,this.setupFrameBufferTexture=Le,this.useMultisampledRTT=ot,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function t2(n,e){function t(i,r=Pr){let s,o=at.getTransfer(r);if(i===zn)return n.UNSIGNED_BYTE;if(i===yh)return n.UNSIGNED_SHORT_4_4_4_4;if(i===_h)return n.UNSIGNED_SHORT_5_5_5_1;if(i===Ry)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Ny)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===Ay)return n.BYTE;if(i===Iy)return n.SHORT;if(i===Ba)return n.UNSIGNED_SHORT;if(i===vh)return n.INT;if(i===Ii)return n.UNSIGNED_INT;if(i===Ri)return n.FLOAT;if(i===nr)return n.HALF_FLOAT;if(i===Py)return n.ALPHA;if(i===Oy)return n.RGB;if(i===hi)return n.RGBA;if(i===Qi)return n.DEPTH_COMPONENT;if(i===xs)return n.DEPTH_STENCIL;if(i===Fy)return n.RED;if(i===xh)return n.RED_INTEGER;if(i===bs)return n.RG;if(i===bh)return n.RG_INTEGER;if(i===Eh)return n.RGBA_INTEGER;if(i===Cl||i===Tl||i===Dl||i===Al)if(o===pt)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Cl)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Tl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===Dl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Al)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Cl)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Tl)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===Dl)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Al)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===Sh||i===Mh||i===wh||i===Ch)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===Sh)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===Mh)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===wh)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Ch)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===Th||i===Dh||i===Ah||i===Ih||i===Rh||i===Il||i===Nh)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===Th||i===Dh)return o===pt?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===Ah)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===Ih)return s.COMPRESSED_R11_EAC;if(i===Rh)return s.COMPRESSED_SIGNED_R11_EAC;if(i===Il)return s.COMPRESSED_RG11_EAC;if(i===Nh)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===Ph||i===Oh||i===Fh||i===Lh||i===kh||i===Uh||i===Bh||i===Vh||i===Hh||i===zh||i===Gh||i===jh||i===Wh||i===$h)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===Ph)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Oh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===Fh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Lh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===kh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Uh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Bh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===Vh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Hh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===zh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Gh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===jh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Wh)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===$h)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===qh||i===Xh||i===Yh)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===qh)return o===pt?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Xh)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Yh)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Zh||i===Kh||i===Rl||i===Jh)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Zh)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Kh)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Rl)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Jh)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Va?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var n2=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,i2=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,t_=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new ul(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Nn({vertexShader:n2,fragmentShader:i2,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new tn(new dl(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},n_=class extends Di{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,d=null,f=null,h=null,g=null,y=typeof XRWebGLBinding<"u",p=new t_,m={},b=t.getContextAttributes(),M=null,S=null,A=[],w=[],I=new Te,_=null,C=new Tn;C.viewport=new it;let O=new Tn;O.viewport=new it;let D=[C,O],L=new hh,G=null,W=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(X){let le=A[X];return le===void 0&&(le=new Da,A[X]=le),le.getTargetRaySpace()},this.getControllerGrip=function(X){let le=A[X];return le===void 0&&(le=new Da,A[X]=le),le.getGripSpace()},this.getHand=function(X){let le=A[X];return le===void 0&&(le=new Da,A[X]=le),le.getHandSpace()};function P(X){let le=w.indexOf(X.inputSource);if(le===-1)return;let ee=A[le];ee!==void 0&&(ee.update(X.inputSource,X.frame,l||o),ee.dispatchEvent({type:X.type,data:X.inputSource}))}function H(){r.removeEventListener("select",P),r.removeEventListener("selectstart",P),r.removeEventListener("selectend",P),r.removeEventListener("squeeze",P),r.removeEventListener("squeezestart",P),r.removeEventListener("squeezeend",P),r.removeEventListener("end",H),r.removeEventListener("inputsourceschange",U);for(let X=0;X<A.length;X++){let le=w[X];le!==null&&(w[X]=null,A[X].disconnect(le))}G=null,W=null,p.reset();for(let X in m)delete m[X];e.setRenderTarget(M),h=null,f=null,d=null,r=null,S=null,Fe.stop(),i.isPresenting=!1,e.setPixelRatio(_),e.setSize(I.width,I.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(X){s=X,i.isPresenting===!0&&Re("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(X){a=X,i.isPresenting===!0&&Re("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(X){l=X},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d===null&&y&&(d=new XRWebGLBinding(r,t)),d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function(X){return cn(this,null,function*(){if(r=X,r!==null){if(M=e.getRenderTarget(),r.addEventListener("select",P),r.addEventListener("selectstart",P),r.addEventListener("selectend",P),r.addEventListener("squeeze",P),r.addEventListener("squeezestart",P),r.addEventListener("squeezeend",P),r.addEventListener("end",H),r.addEventListener("inputsourceschange",U),b.xrCompatible!==!0&&(yield t.makeXRCompatible()),_=e.getPixelRatio(),e.getSize(I),y&&"createProjectionLayer"in XRWebGLBinding.prototype){let ee=null,Oe=null,We=null;b.depth&&(We=b.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ee=b.stencil?xs:Qi,Oe=b.stencil?Va:Ii);let Le={colorFormat:t.RGBA8,depthFormat:We,scaleFactor:s};d=this.getBinding(),f=d.createProjectionLayer(Le),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),S=new Kn(f.textureWidth,f.textureHeight,{format:hi,type:zn,depthTexture:new Nr(f.textureWidth,f.textureHeight,Oe,void 0,void 0,void 0,void 0,void 0,void 0,ee),stencilBuffer:b.stencil,colorSpace:e.outputColorSpace,samples:b.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1,resolveStencilBuffer:f.ignoreDepthValues===!1})}else{let ee={antialias:b.antialias,alpha:!0,depth:b.depth,stencil:b.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,ee),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),S=new Kn(h.framebufferWidth,h.framebufferHeight,{format:hi,type:zn,colorSpace:e.outputColorSpace,stencilBuffer:b.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=yield r.requestReferenceSpace(a),Fe.setContext(r),Fe.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return p.getDepthTexture()};function U(X){for(let le=0;le<X.removed.length;le++){let ee=X.removed[le],Oe=w.indexOf(ee);Oe>=0&&(w[Oe]=null,A[Oe].disconnect(ee))}for(let le=0;le<X.added.length;le++){let ee=X.added[le],Oe=w.indexOf(ee);if(Oe===-1){for(let Le=0;Le<A.length;Le++)if(Le>=w.length){w.push(ee),Oe=Le;break}else if(w[Le]===null){w[Le]=ee,Oe=Le;break}if(Oe===-1)break}let We=A[Oe];We&&We.connect(ee)}}let J=new T,Q=new T;function ce(X,le,ee){J.setFromMatrixPosition(le.matrixWorld),Q.setFromMatrixPosition(ee.matrixWorld);let Oe=J.distanceTo(Q),We=le.projectionMatrix.elements,Le=ee.projectionMatrix.elements,Lt=We[14]/(We[10]-1),st=We[14]/(We[10]+1),vt=(We[9]+1)/We[5],Ot=(We[9]-1)/We[5],tt=(We[8]-1)/We[0],Kt=(Le[8]+1)/Le[0],kt=Lt*tt,jn=Lt*Kt,N=Oe/(-tt+Kt),Jt=N*-tt;if(le.matrixWorld.decompose(X.position,X.quaternion,X.scale),X.translateX(Jt),X.translateZ(N),X.matrixWorld.compose(X.position,X.quaternion,X.scale),X.matrixWorldInverse.copy(X.matrixWorld).invert(),We[10]===-1)X.projectionMatrix.copy(le.projectionMatrix),X.projectionMatrixInverse.copy(le.projectionMatrixInverse);else{let ot=Lt+N,At=st+N,ue=kt-Jt,Vt=jn+(Oe-Jt),E=vt*st/At*ot,v=Ot*st/At*ot;X.projectionMatrix.makePerspective(ue,Vt,E,v,ot,At),X.projectionMatrixInverse.copy(X.projectionMatrix).invert()}}function xe(X,le){le===null?X.matrixWorld.copy(X.matrix):X.matrixWorld.multiplyMatrices(le.matrixWorld,X.matrix),X.matrixWorldInverse.copy(X.matrixWorld).invert()}this.updateCamera=function(X){if(r===null)return;let le=X.near,ee=X.far;p.texture!==null&&(p.depthNear>0&&(le=p.depthNear),p.depthFar>0&&(ee=p.depthFar)),L.near=O.near=C.near=le,L.far=O.far=C.far=ee,(G!==L.near||W!==L.far)&&(r.updateRenderState({depthNear:L.near,depthFar:L.far}),G=L.near,W=L.far),L.layers.mask=X.layers.mask|6,C.layers.mask=L.layers.mask&-5,O.layers.mask=L.layers.mask&-3;let Oe=X.parent,We=L.cameras;xe(L,Oe);for(let Le=0;Le<We.length;Le++)xe(We[Le],Oe);We.length===2?ce(L,C,O):L.projectionMatrix.copy(C.projectionMatrix),Se(X,L,Oe)};function Se(X,le,ee){ee===null?X.matrix.copy(le.matrixWorld):(X.matrix.copy(ee.matrixWorld),X.matrix.invert(),X.matrix.multiply(le.matrixWorld)),X.matrix.decompose(X.position,X.quaternion,X.scale),X.updateMatrixWorld(!0),X.projectionMatrix.copy(le.projectionMatrix),X.projectionMatrixInverse.copy(le.projectionMatrixInverse),X.isPerspectiveCamera&&(X.fov=Ma*2*Math.atan(1/X.projectionMatrix.elements[5]),X.zoom=1)}this.getCamera=function(){return L},this.getFoveation=function(){if(!(f===null&&h===null))return c},this.setFoveation=function(X){c=X,f!==null&&(f.fixedFoveation=X),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=X)},this.hasDepthSensing=function(){return p.texture!==null},this.getDepthSensingMesh=function(){return p.getMesh(L)},this.getCameraTexture=function(X){return m[X]};let rt=null;function Ke(X,le){if(u=le.getViewerPose(l||o),g=le,u!==null){let ee=u.views;h!==null&&(e.setRenderTargetFramebuffer(S,h.framebuffer),e.setRenderTarget(S));let Oe=!1;ee.length!==L.cameras.length&&(L.cameras.length=0,Oe=!0);for(let st=0;st<ee.length;st++){let vt=ee[st],Ot=null;if(h!==null)Ot=h.getViewport(vt);else{let Kt=d.getViewSubImage(f,vt);Ot=Kt.viewport,st===0&&(e.setRenderTargetTextures(S,Kt.colorTexture,Kt.depthStencilTexture),e.setRenderTarget(S))}let tt=D[st];tt===void 0&&(tt=new Tn,tt.layers.enable(st),tt.viewport=new it,D[st]=tt),tt.matrix.fromArray(vt.transform.matrix),tt.matrix.decompose(tt.position,tt.quaternion,tt.scale),tt.projectionMatrix.fromArray(vt.projectionMatrix),tt.projectionMatrixInverse.copy(tt.projectionMatrix).invert(),tt.viewport.set(Ot.x,Ot.y,Ot.width,Ot.height),st===0&&(L.matrix.copy(tt.matrix),L.matrix.decompose(L.position,L.quaternion,L.scale)),Oe===!0&&L.cameras.push(tt)}let We=r.enabledFeatures;if(We&&We.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&y){d=i.getBinding();let st=d.getDepthInformation(ee[0]);st&&st.isValid&&st.texture&&p.init(st,r.renderState)}if(We&&We.includes("camera-access")&&y){e.state.unbindTexture(),d=i.getBinding();for(let st=0;st<ee.length;st++){let vt=ee[st].camera;if(vt){let Ot=m[vt];Ot||(Ot=new ul,m[vt]=Ot);let tt=d.getCameraImage(vt);Ot.sourceTexture=tt}}}}for(let ee=0;ee<A.length;ee++){let Oe=w[ee],We=A[ee];Oe!==null&&We!==void 0&&We.update(Oe,le,l||o)}rt&&rt(X,le),le.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:le}),g=null}let Fe=new AM;Fe.setAnimationLoop(Ke),this.setAnimationLoop=function(X){rt=X},this.dispose=function(){}}},r2=new Dt,FM=new je;FM.set(-1,0,0,0,1,0,0,0,1);function s2(n,e){function t(p,m){p.matrixAutoUpdate===!0&&p.updateMatrix(),m.value.copy(p.matrix)}function i(p,m){m.color.getRGB(p.fogColor.value,Uy(n)),m.isFog?(p.fogNear.value=m.near,p.fogFar.value=m.far):m.isFogExp2&&(p.fogDensity.value=m.density)}function r(p,m,b,M,S){m.isNodeMaterial?m.uniformsNeedUpdate=!1:m.isMeshBasicMaterial?s(p,m):m.isMeshLambertMaterial?(s(p,m),m.envMap&&(p.envMapIntensity.value=m.envMapIntensity)):m.isMeshToonMaterial?(s(p,m),d(p,m)):m.isMeshPhongMaterial?(s(p,m),u(p,m),m.envMap&&(p.envMapIntensity.value=m.envMapIntensity)):m.isMeshStandardMaterial?(s(p,m),f(p,m),m.isMeshPhysicalMaterial&&h(p,m,S)):m.isMeshMatcapMaterial?(s(p,m),g(p,m)):m.isMeshDepthMaterial?s(p,m):m.isMeshDistanceMaterial?(s(p,m),y(p,m)):m.isMeshNormalMaterial?s(p,m):m.isLineBasicMaterial?(o(p,m),m.isLineDashedMaterial&&a(p,m)):m.isPointsMaterial?c(p,m,b,M):m.isSpriteMaterial?l(p,m):m.isShadowMaterial?(p.color.value.copy(m.color),p.opacity.value=m.opacity):m.isShaderMaterial&&(m.uniformsNeedUpdate=!1)}function s(p,m){p.opacity.value=m.opacity,m.color&&p.diffuse.value.copy(m.color),m.emissive&&p.emissive.value.copy(m.emissive).multiplyScalar(m.emissiveIntensity),m.map&&(p.map.value=m.map,t(m.map,p.mapTransform)),m.alphaMap&&(p.alphaMap.value=m.alphaMap,t(m.alphaMap,p.alphaMapTransform)),m.bumpMap&&(p.bumpMap.value=m.bumpMap,t(m.bumpMap,p.bumpMapTransform),p.bumpScale.value=m.bumpScale,m.side===Pn&&(p.bumpScale.value*=-1)),m.normalMap&&(p.normalMap.value=m.normalMap,t(m.normalMap,p.normalMapTransform),p.normalScale.value.copy(m.normalScale),m.side===Pn&&p.normalScale.value.negate()),m.displacementMap&&(p.displacementMap.value=m.displacementMap,t(m.displacementMap,p.displacementMapTransform),p.displacementScale.value=m.displacementScale,p.displacementBias.value=m.displacementBias),m.emissiveMap&&(p.emissiveMap.value=m.emissiveMap,t(m.emissiveMap,p.emissiveMapTransform)),m.specularMap&&(p.specularMap.value=m.specularMap,t(m.specularMap,p.specularMapTransform)),m.alphaTest>0&&(p.alphaTest.value=m.alphaTest);let b=e.get(m),M=b.envMap,S=b.envMapRotation;M&&(p.envMap.value=M,p.envMapRotation.value.setFromMatrix4(r2.makeRotationFromEuler(S)).transpose(),M.isCubeTexture&&M.isRenderTargetTexture===!1&&p.envMapRotation.value.premultiply(FM),p.reflectivity.value=m.reflectivity,p.ior.value=m.ior,p.refractionRatio.value=m.refractionRatio),m.lightMap&&(p.lightMap.value=m.lightMap,p.lightMapIntensity.value=m.lightMapIntensity,t(m.lightMap,p.lightMapTransform)),m.aoMap&&(p.aoMap.value=m.aoMap,p.aoMapIntensity.value=m.aoMapIntensity,t(m.aoMap,p.aoMapTransform))}function o(p,m){p.diffuse.value.copy(m.color),p.opacity.value=m.opacity,m.map&&(p.map.value=m.map,t(m.map,p.mapTransform))}function a(p,m){p.dashSize.value=m.dashSize,p.totalSize.value=m.dashSize+m.gapSize,p.scale.value=m.scale}function c(p,m,b,M){p.diffuse.value.copy(m.color),p.opacity.value=m.opacity,p.size.value=m.size*b,p.scale.value=M*.5,m.map&&(p.map.value=m.map,t(m.map,p.uvTransform)),m.alphaMap&&(p.alphaMap.value=m.alphaMap,t(m.alphaMap,p.alphaMapTransform)),m.alphaTest>0&&(p.alphaTest.value=m.alphaTest)}function l(p,m){p.diffuse.value.copy(m.color),p.opacity.value=m.opacity,p.rotation.value=m.rotation,m.map&&(p.map.value=m.map,t(m.map,p.mapTransform)),m.alphaMap&&(p.alphaMap.value=m.alphaMap,t(m.alphaMap,p.alphaMapTransform)),m.alphaTest>0&&(p.alphaTest.value=m.alphaTest)}function u(p,m){p.specular.value.copy(m.specular),p.shininess.value=Math.max(m.shininess,1e-4)}function d(p,m){m.gradientMap&&(p.gradientMap.value=m.gradientMap)}function f(p,m){p.metalness.value=m.metalness,m.metalnessMap&&(p.metalnessMap.value=m.metalnessMap,t(m.metalnessMap,p.metalnessMapTransform)),p.roughness.value=m.roughness,m.roughnessMap&&(p.roughnessMap.value=m.roughnessMap,t(m.roughnessMap,p.roughnessMapTransform)),m.envMap&&(p.envMapIntensity.value=m.envMapIntensity)}function h(p,m,b){p.ior.value=m.ior,m.sheen>0&&(p.sheenColor.value.copy(m.sheenColor).multiplyScalar(m.sheen),p.sheenRoughness.value=m.sheenRoughness,m.sheenColorMap&&(p.sheenColorMap.value=m.sheenColorMap,t(m.sheenColorMap,p.sheenColorMapTransform)),m.sheenRoughnessMap&&(p.sheenRoughnessMap.value=m.sheenRoughnessMap,t(m.sheenRoughnessMap,p.sheenRoughnessMapTransform))),m.clearcoat>0&&(p.clearcoat.value=m.clearcoat,p.clearcoatRoughness.value=m.clearcoatRoughness,m.clearcoatMap&&(p.clearcoatMap.value=m.clearcoatMap,t(m.clearcoatMap,p.clearcoatMapTransform)),m.clearcoatRoughnessMap&&(p.clearcoatRoughnessMap.value=m.clearcoatRoughnessMap,t(m.clearcoatRoughnessMap,p.clearcoatRoughnessMapTransform)),m.clearcoatNormalMap&&(p.clearcoatNormalMap.value=m.clearcoatNormalMap,t(m.clearcoatNormalMap,p.clearcoatNormalMapTransform),p.clearcoatNormalScale.value.copy(m.clearcoatNormalScale),m.side===Pn&&p.clearcoatNormalScale.value.negate())),m.dispersion>0&&(p.dispersion.value=m.dispersion),m.iridescence>0&&(p.iridescence.value=m.iridescence,p.iridescenceIOR.value=m.iridescenceIOR,p.iridescenceThicknessMinimum.value=m.iridescenceThicknessRange[0],p.iridescenceThicknessMaximum.value=m.iridescenceThicknessRange[1],m.iridescenceMap&&(p.iridescenceMap.value=m.iridescenceMap,t(m.iridescenceMap,p.iridescenceMapTransform)),m.iridescenceThicknessMap&&(p.iridescenceThicknessMap.value=m.iridescenceThicknessMap,t(m.iridescenceThicknessMap,p.iridescenceThicknessMapTransform))),m.transmission>0&&(p.transmission.value=m.transmission,p.transmissionSamplerMap.value=b.texture,p.transmissionSamplerSize.value.set(b.width,b.height),m.transmissionMap&&(p.transmissionMap.value=m.transmissionMap,t(m.transmissionMap,p.transmissionMapTransform)),p.thickness.value=m.thickness,m.thicknessMap&&(p.thicknessMap.value=m.thicknessMap,t(m.thicknessMap,p.thicknessMapTransform)),p.attenuationDistance.value=m.attenuationDistance,p.attenuationColor.value.copy(m.attenuationColor)),m.anisotropy>0&&(p.anisotropyVector.value.set(m.anisotropy*Math.cos(m.anisotropyRotation),m.anisotropy*Math.sin(m.anisotropyRotation)),m.anisotropyMap&&(p.anisotropyMap.value=m.anisotropyMap,t(m.anisotropyMap,p.anisotropyMapTransform))),p.specularIntensity.value=m.specularIntensity,p.specularColor.value.copy(m.specularColor),m.specularColorMap&&(p.specularColorMap.value=m.specularColorMap,t(m.specularColorMap,p.specularColorMapTransform)),m.specularIntensityMap&&(p.specularIntensityMap.value=m.specularIntensityMap,t(m.specularIntensityMap,p.specularIntensityMapTransform))}function g(p,m){m.matcap&&(p.matcap.value=m.matcap)}function y(p,m){let b=e.get(m).light;p.referencePosition.value.setFromMatrixPosition(b.matrixWorld),p.nearDistance.value=b.shadow.camera.near,p.farDistance.value=b.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function o2(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(b,M){let S=M.program;i.uniformBlockBinding(b,S)}function l(b,M){let S=r[b.id];S===void 0&&(g(b),S=u(b),r[b.id]=S,b.addEventListener("dispose",p));let A=M.program;i.updateUBOMapping(b,A);let w=e.render.frame;s[b.id]!==w&&(f(b),s[b.id]=w)}function u(b){let M=d();b.__bindingPointIndex=M;let S=n.createBuffer(),A=b.__size,w=b.usage;return n.bindBuffer(n.UNIFORM_BUFFER,S),n.bufferData(n.UNIFORM_BUFFER,A,w),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,M,S),S}function d(){for(let b=0;b<a;b++)if(o.indexOf(b)===-1)return o.push(b),b;return Pe("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function f(b){let M=r[b.id],S=b.uniforms,A=b.__cache;n.bindBuffer(n.UNIFORM_BUFFER,M);for(let w=0,I=S.length;w<I;w++){let _=Array.isArray(S[w])?S[w]:[S[w]];for(let C=0,O=_.length;C<O;C++){let D=_[C];if(h(D,w,C,A)===!0){let L=D.__offset,G=Array.isArray(D.value)?D.value:[D.value],W=0;for(let P=0;P<G.length;P++){let H=G[P],U=y(H);typeof H=="number"||typeof H=="boolean"?(D.__data[0]=H,n.bufferSubData(n.UNIFORM_BUFFER,L+W,D.__data)):H.isMatrix3?(D.__data[0]=H.elements[0],D.__data[1]=H.elements[1],D.__data[2]=H.elements[2],D.__data[3]=0,D.__data[4]=H.elements[3],D.__data[5]=H.elements[4],D.__data[6]=H.elements[5],D.__data[7]=0,D.__data[8]=H.elements[6],D.__data[9]=H.elements[7],D.__data[10]=H.elements[8],D.__data[11]=0):ArrayBuffer.isView(H)?D.__data.set(new H.constructor(H.buffer,H.byteOffset,D.__data.length)):(H.toArray(D.__data,W),W+=U.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,L,D.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(b,M,S,A){let w=b.value,I=M+"_"+S;if(A[I]===void 0)return typeof w=="number"||typeof w=="boolean"?A[I]=w:ArrayBuffer.isView(w)?A[I]=w.slice():A[I]=w.clone(),!0;{let _=A[I];if(typeof w=="number"||typeof w=="boolean"){if(_!==w)return A[I]=w,!0}else{if(ArrayBuffer.isView(w))return!0;if(_.equals(w)===!1)return _.copy(w),!0}}return!1}function g(b){let M=b.uniforms,S=0,A=16;for(let I=0,_=M.length;I<_;I++){let C=Array.isArray(M[I])?M[I]:[M[I]];for(let O=0,D=C.length;O<D;O++){let L=C[O],G=Array.isArray(L.value)?L.value:[L.value];for(let W=0,P=G.length;W<P;W++){let H=G[W],U=y(H),J=S%A,Q=J%U.boundary,ce=J+Q;S+=Q,ce!==0&&A-ce<U.storage&&(S+=A-ce),L.__data=new Float32Array(U.storage/Float32Array.BYTES_PER_ELEMENT),L.__offset=S,S+=U.storage}}}let w=S%A;return w>0&&(S+=A-w),b.__size=S,b.__cache={},this}function y(b){let M={boundary:0,storage:0};return typeof b=="number"||typeof b=="boolean"?(M.boundary=4,M.storage=4):b.isVector2?(M.boundary=8,M.storage=8):b.isVector3||b.isColor?(M.boundary=16,M.storage=12):b.isVector4?(M.boundary=16,M.storage=16):b.isMatrix3?(M.boundary=48,M.storage=48):b.isMatrix4?(M.boundary=64,M.storage=64):b.isTexture?Re("WebGLRenderer: Texture samplers can not be part of an uniforms group."):ArrayBuffer.isView(b)?(M.boundary=16,M.storage=b.byteLength):Re("WebGLRenderer: Unsupported uniform value type.",b),M}function p(b){let M=b.target;M.removeEventListener("dispose",p);let S=o.indexOf(M.__bindingPointIndex);o.splice(S,1),n.deleteBuffer(r[M.id]),delete r[M.id],delete s[M.id]}function m(){for(let b in r)n.deleteBuffer(r[b]);o=[],r={},s={}}return{bind:c,update:l,dispose:m}}var a2=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),rr=null;function c2(){return rr===null&&(rr=new Xf(a2,16,16,bs,nr),rr.name="DFG_LUT",rr.minFilter=hn,rr.magFilter=hn,rr.wrapS=Ji,rr.wrapT=Ji,rr.generateMipmaps=!1,rr.needsUpdate=!0),rr}var op=class{constructor(e={}){let{canvas:t=tM(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reversedDepthBuffer:f=!1,outputBufferType:h=zn}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let y=h,p=new Set([Eh,bh,xh]),m=new Set([zn,Ii,Ba,Va,yh,_h]),b=new Uint32Array(4),M=new Int32Array(4),S=new T,A=null,w=null,I=[],_=[],C=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=Ai,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let O=this,D=!1,L=null;this._outputColorSpace=Cn;let G=0,W=0,P=null,H=-1,U=null,J=new it,Q=new it,ce=null,xe=new Ve(0),Se=0,rt=t.width,Ke=t.height,Fe=1,X=null,le=null,ee=new it(0,0,rt,Ke),Oe=new it(0,0,rt,Ke),We=!1,Le=new Ia,Lt=!1,st=!1,vt=new Dt,Ot=new T,tt=new it,Kt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},kt=!1;function jn(){return P===null?Fe:1}let N=i;function Jt(x,F){return t.getContext(x,F)}try{let x={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"184"}`),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",Me,!1),t.addEventListener("webglcontextcreationerror",$e,!1),N===null){let F="webgl2";if(N=Jt(F,x),N===null)throw Jt(F)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw Pe("WebGLRenderer: "+x.message),x}let ot,At,ue,Vt,E,v,k,q,K,te,oe,j,Y,he,ye,re,ne,He,Xe,dt,R,ie,$;function me(){ot=new mF(N),ot.init(),R=new t2(N,ot),At=new aF(N,ot,e,R),ue=new QL(N,ot),At.reversedDepthBuffer&&f&&ue.buffers.depth.setReversed(!0),Vt=new yF(N),E=new BL,v=new e2(N,ot,ue,E,At,R,Vt),k=new pF(O),q=new EN(N),ie=new sF(N,q),K=new gF(N,q,Vt,ie),te=new xF(N,K,q,ie,Vt),He=new _F(N,At,v),ye=new cF(E),oe=new UL(O,k,ot,At,ie,ye),j=new s2(O,E),Y=new HL,he=new qL(ot),ne=new rF(O,k,ue,te,g,c),re=new JL(O,te,At),$=new o2(N,Vt,At,ue),Xe=new oF(N,ot,Vt),dt=new vF(N,ot,Vt),Vt.programs=oe.programs,O.capabilities=At,O.extensions=ot,O.properties=E,O.renderLists=Y,O.shadowMap=re,O.state=ue,O.info=Vt}me(),y!==zn&&(C=new EF(y,t.width,t.height,r,s));let se=new n_(O,N);this.xr=se,this.getContext=function(){return N},this.getContextAttributes=function(){return N.getContextAttributes()},this.forceContextLoss=function(){let x=ot.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=ot.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return Fe},this.setPixelRatio=function(x){x!==void 0&&(Fe=x,this.setSize(rt,Ke,!1))},this.getSize=function(x){return x.set(rt,Ke)},this.setSize=function(x,F,z=!0){if(se.isPresenting){Re("WebGLRenderer: Can't change size while VR device is presenting.");return}rt=x,Ke=F,t.width=Math.floor(x*Fe),t.height=Math.floor(F*Fe),z===!0&&(t.style.width=x+"px",t.style.height=F+"px"),C!==null&&C.setSize(t.width,t.height),this.setViewport(0,0,x,F)},this.getDrawingBufferSize=function(x){return x.set(rt*Fe,Ke*Fe).floor()},this.setDrawingBufferSize=function(x,F,z){rt=x,Ke=F,Fe=z,t.width=Math.floor(x*z),t.height=Math.floor(F*z),this.setViewport(0,0,x,F)},this.setEffects=function(x){if(y===zn){Pe("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(x){for(let F=0;F<x.length;F++)if(x[F].isOutputPass===!0){Re("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}C.setEffects(x||[])},this.getCurrentViewport=function(x){return x.copy(J)},this.getViewport=function(x){return x.copy(ee)},this.setViewport=function(x,F,z,B){x.isVector4?ee.set(x.x,x.y,x.z,x.w):ee.set(x,F,z,B),ue.viewport(J.copy(ee).multiplyScalar(Fe).round())},this.getScissor=function(x){return x.copy(Oe)},this.setScissor=function(x,F,z,B){x.isVector4?Oe.set(x.x,x.y,x.z,x.w):Oe.set(x,F,z,B),ue.scissor(Q.copy(Oe).multiplyScalar(Fe).round())},this.getScissorTest=function(){return We},this.setScissorTest=function(x){ue.setScissorTest(We=x)},this.setOpaqueSort=function(x){X=x},this.setTransparentSort=function(x){le=x},this.getClearColor=function(x){return x.copy(ne.getClearColor())},this.setClearColor=function(){ne.setClearColor(...arguments)},this.getClearAlpha=function(){return ne.getClearAlpha()},this.setClearAlpha=function(){ne.setClearAlpha(...arguments)},this.clear=function(x=!0,F=!0,z=!0){let B=0;if(x){let V=!1;if(P!==null){let fe=P.texture.format;V=p.has(fe)}if(V){let fe=P.texture.type,_e=m.has(fe),de=ne.getClearColor(),be=ne.getClearAlpha(),we=de.r,qe=de.g,Qe=de.b;_e?(b[0]=we,b[1]=qe,b[2]=Qe,b[3]=be,N.clearBufferuiv(N.COLOR,0,b)):(M[0]=we,M[1]=qe,M[2]=Qe,M[3]=be,N.clearBufferiv(N.COLOR,0,M))}else B|=N.COLOR_BUFFER_BIT}F&&(B|=N.DEPTH_BUFFER_BIT,this.state.buffers.depth.setMask(!0)),z&&(B|=N.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),B!==0&&N.clear(B)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.setNodesHandler=function(x){x.setRenderer(this),L=x},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",Me,!1),t.removeEventListener("webglcontextcreationerror",$e,!1),ne.dispose(),Y.dispose(),he.dispose(),E.dispose(),k.dispose(),te.dispose(),ie.dispose(),$.dispose(),oe.dispose(),se.dispose(),se.removeEventListener("sessionstart",c_),se.removeEventListener("sessionend",l_),ws.stop()};function Z(x){x.preventDefault(),el("WebGLRenderer: Context Lost."),D=!0}function Me(){el("WebGLRenderer: Context Restored."),D=!1;let x=Vt.autoReset,F=re.enabled,z=re.autoUpdate,B=re.needsUpdate,V=re.type;me(),Vt.autoReset=x,re.enabled=F,re.autoUpdate=z,re.needsUpdate=B,re.type=V}function $e(x){Pe("WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function $t(x){let F=x.target;F.removeEventListener("dispose",$t),yt(F)}function yt(x){cr(x),E.remove(x)}function cr(x){let F=E.get(x).programs;F!==void 0&&(F.forEach(function(z){oe.releaseProgram(z)}),x.isShaderMaterial&&oe.releaseShaderCache(x))}this.renderBufferDirect=function(x,F,z,B,V,fe){F===null&&(F=Kt);let _e=V.isMesh&&V.matrixWorld.determinant()<0,de=KM(x,F,z,B,V);ue.setMaterial(B,_e);let be=z.index,we=1;if(B.wireframe===!0){if(be=K.getWireframeAttribute(z),be===void 0)return;we=2}let qe=z.drawRange,Qe=z.attributes.position,De=qe.start*we,_t=(qe.start+qe.count)*we;fe!==null&&(De=Math.max(De,fe.start*we),_t=Math.min(_t,(fe.start+fe.count)*we)),be!==null?(De=Math.max(De,0),_t=Math.min(_t,be.count)):Qe!=null&&(De=Math.max(De,0),_t=Math.min(_t,Qe.count));let qt=_t-De;if(qt<0||qt===1/0)return;ie.setup(V,B,de,z,be);let Ht,Mt=Xe;if(be!==null&&(Ht=q.get(be),Mt=dt,Mt.setIndex(Ht)),V.isMesh)B.wireframe===!0?(ue.setLineWidth(B.wireframeLinewidth*jn()),Mt.setMode(N.LINES)):Mt.setMode(N.TRIANGLES);else if(V.isLine){let yn=B.linewidth;yn===void 0&&(yn=1),ue.setLineWidth(yn*jn()),V.isLineSegments?Mt.setMode(N.LINES):V.isLineLoop?Mt.setMode(N.LINE_LOOP):Mt.setMode(N.LINE_STRIP)}else V.isPoints?Mt.setMode(N.POINTS):V.isSprite&&Mt.setMode(N.TRIANGLES);if(V.isBatchedMesh)if(ot.get("WEBGL_multi_draw"))Mt.renderMultiDraw(V._multiDrawStarts,V._multiDrawCounts,V._multiDrawCount);else{let yn=V._multiDrawStarts,ve=V._multiDrawCounts,Wn=V._multiDrawCount,lt=be?q.get(be).bytesPerElement:1,ni=E.get(B).currentProgram.getUniforms();for(let Oi=0;Oi<Wn;Oi++)ni.setValue(N,"_gl_DrawID",Oi),Mt.render(yn[Oi]/lt,ve[Oi])}else if(V.isInstancedMesh)Mt.renderInstances(De,qt,V.count);else if(z.isInstancedBufferGeometry){let yn=z._maxInstanceCount!==void 0?z._maxInstanceCount:1/0,ve=Math.min(z.instanceCount,yn);Mt.renderInstances(De,qt,ve)}else Mt.render(De,qt)};function Pi(x,F,z){x.transparent===!0&&x.side===fi&&x.forceSinglePass===!1?(x.side=Pn,x.needsUpdate=!0,Ul(x,F,z),x.side=Rr,x.needsUpdate=!0,Ul(x,F,z),x.side=fi):Ul(x,F,z)}this.compile=function(x,F,z=null){z===null&&(z=x),w=he.get(z),w.init(F),_.push(w),z.traverseVisible(function(V){V.isLight&&V.layers.test(F.layers)&&(w.pushLight(V),V.castShadow&&w.pushShadow(V))}),x!==z&&x.traverseVisible(function(V){V.isLight&&V.layers.test(F.layers)&&(w.pushLight(V),V.castShadow&&w.pushShadow(V))}),w.setupLights();let B=new Set;return x.traverse(function(V){if(!(V.isMesh||V.isPoints||V.isLine||V.isSprite))return;let fe=V.material;if(fe)if(Array.isArray(fe))for(let _e=0;_e<fe.length;_e++){let de=fe[_e];Pi(de,z,V),B.add(de)}else Pi(fe,z,V),B.add(fe)}),w=_.pop(),B},this.compileAsync=function(x,F,z=null){let B=this.compile(x,F,z);return new Promise(V=>{function fe(){if(B.forEach(function(_e){E.get(_e).currentProgram.isReady()&&B.delete(_e)}),B.size===0){V(x);return}setTimeout(fe,10)}ot.get("KHR_parallel_shader_compile")!==null?fe():setTimeout(fe,10)})};let xp=null;function YM(x){xp&&xp(x)}function c_(){ws.stop()}function l_(){ws.start()}let ws=new AM;ws.setAnimationLoop(YM),typeof self<"u"&&ws.setContext(self),this.setAnimationLoop=function(x){xp=x,se.setAnimationLoop(x),x===null?ws.stop():ws.start()},se.addEventListener("sessionstart",c_),se.addEventListener("sessionend",l_),this.render=function(x,F){if(F!==void 0&&F.isCamera!==!0){Pe("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(D===!0)return;L!==null&&L.renderStart(x,F);let z=se.enabled===!0&&se.isPresenting===!0,B=C!==null&&(P===null||z)&&C.begin(O,P);if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),F.parent===null&&F.matrixWorldAutoUpdate===!0&&F.updateMatrixWorld(),se.enabled===!0&&se.isPresenting===!0&&(C===null||C.isCompositing()===!1)&&(se.cameraAutoUpdate===!0&&se.updateCamera(F),F=se.getCamera()),x.isScene===!0&&x.onBeforeRender(O,x,F,P),w=he.get(x,_.length),w.init(F),w.state.textureUnits=v.getTextureUnits(),_.push(w),vt.multiplyMatrices(F.projectionMatrix,F.matrixWorldInverse),Le.setFromProjectionMatrix(vt,Ti,F.reversedDepth),st=this.localClippingEnabled,Lt=ye.init(this.clippingPlanes,st),A=Y.get(x,I.length),A.init(),I.push(A),se.enabled===!0&&se.isPresenting===!0){let _e=O.xr.getDepthSensingMesh();_e!==null&&bp(_e,F,-1/0,O.sortObjects)}bp(x,F,0,O.sortObjects),A.finish(),O.sortObjects===!0&&A.sort(X,le),kt=se.enabled===!1||se.isPresenting===!1||se.hasDepthSensing()===!1,kt&&ne.addToRenderList(A,x),this.info.render.frame++,Lt===!0&&ye.beginShadows();let V=w.state.shadowsArray;if(re.render(V,x,F),Lt===!0&&ye.endShadows(),this.info.autoReset===!0&&this.info.reset(),(B&&C.hasRenderPass())===!1){let _e=A.opaque,de=A.transmissive;if(w.setupLights(),F.isArrayCamera){let be=F.cameras;if(de.length>0)for(let we=0,qe=be.length;we<qe;we++){let Qe=be[we];d_(_e,de,x,Qe)}kt&&ne.render(x);for(let we=0,qe=be.length;we<qe;we++){let Qe=be[we];u_(A,x,Qe,Qe.viewport)}}else de.length>0&&d_(_e,de,x,F),kt&&ne.render(x),u_(A,x,F)}P!==null&&W===0&&(v.updateMultisampleRenderTarget(P),v.updateRenderTargetMipmap(P)),B&&C.end(O),x.isScene===!0&&x.onAfterRender(O,x,F),ie.resetDefaultState(),H=-1,U=null,_.pop(),_.length>0?(w=_[_.length-1],v.setTextureUnits(w.state.textureUnits),Lt===!0&&ye.setGlobalState(O.clippingPlanes,w.state.camera)):w=null,I.pop(),I.length>0?A=I[I.length-1]:A=null,L!==null&&L.renderEnd()};function bp(x,F,z,B){if(x.visible===!1)return;if(x.layers.test(F.layers)){if(x.isGroup)z=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(F);else if(x.isLightProbeGrid)w.pushLightProbeGrid(x);else if(x.isLight)w.pushLight(x),x.castShadow&&w.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||Le.intersectsSprite(x)){B&&tt.setFromMatrixPosition(x.matrixWorld).applyMatrix4(vt);let _e=te.update(x),de=x.material;de.visible&&A.push(x,_e,de,z,tt.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||Le.intersectsObject(x))){let _e=te.update(x),de=x.material;if(B&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),tt.copy(x.boundingSphere.center)):(_e.boundingSphere===null&&_e.computeBoundingSphere(),tt.copy(_e.boundingSphere.center)),tt.applyMatrix4(x.matrixWorld).applyMatrix4(vt)),Array.isArray(de)){let be=_e.groups;for(let we=0,qe=be.length;we<qe;we++){let Qe=be[we],De=de[Qe.materialIndex];De&&De.visible&&A.push(x,_e,De,z,tt.z,Qe)}}else de.visible&&A.push(x,_e,de,z,tt.z,null)}}let fe=x.children;for(let _e=0,de=fe.length;_e<de;_e++)bp(fe[_e],F,z,B)}function u_(x,F,z,B){let{opaque:V,transmissive:fe,transparent:_e}=x;w.setupLightsView(z),Lt===!0&&ye.setGlobalState(O.clippingPlanes,z),B&&ue.viewport(J.copy(B)),V.length>0&&kl(V,F,z),fe.length>0&&kl(fe,F,z),_e.length>0&&kl(_e,F,z),ue.buffers.depth.setTest(!0),ue.buffers.depth.setMask(!0),ue.buffers.color.setMask(!0),ue.setPolygonOffset(!1)}function d_(x,F,z,B){if((z.isScene===!0?z.overrideMaterial:null)!==null)return;if(w.state.transmissionRenderTarget[B.id]===void 0){let De=ot.has("EXT_color_buffer_half_float")||ot.has("EXT_color_buffer_float");w.state.transmissionRenderTarget[B.id]=new Kn(1,1,{generateMipmaps:!0,type:De?nr:zn,minFilter:_s,samples:Math.max(4,At.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:at.workingColorSpace})}let fe=w.state.transmissionRenderTarget[B.id],_e=B.viewport||J;fe.setSize(_e.z*O.transmissionResolutionScale,_e.w*O.transmissionResolutionScale);let de=O.getRenderTarget(),be=O.getActiveCubeFace(),we=O.getActiveMipmapLevel();O.setRenderTarget(fe),O.getClearColor(xe),Se=O.getClearAlpha(),Se<1&&O.setClearColor(16777215,.5),O.clear(),kt&&ne.render(z);let qe=O.toneMapping;O.toneMapping=Ai;let Qe=B.viewport;if(B.viewport!==void 0&&(B.viewport=void 0),w.setupLightsView(B),Lt===!0&&ye.setGlobalState(O.clippingPlanes,B),kl(x,z,B),v.updateMultisampleRenderTarget(fe),v.updateRenderTargetMipmap(fe),ot.has("WEBGL_multisampled_render_to_texture")===!1){let De=!1;for(let _t=0,qt=F.length;_t<qt;_t++){let Ht=F[_t],{object:Mt,geometry:yn,material:ve,group:Wn}=Ht;if(ve.side===fi&&Mt.layers.test(B.layers)){let lt=ve.side;ve.side=Pn,ve.needsUpdate=!0,f_(Mt,z,B,yn,ve,Wn),ve.side=lt,ve.needsUpdate=!0,De=!0}}De===!0&&(v.updateMultisampleRenderTarget(fe),v.updateRenderTargetMipmap(fe))}O.setRenderTarget(de,be,we),O.setClearColor(xe,Se),Qe!==void 0&&(B.viewport=Qe),O.toneMapping=qe}function kl(x,F,z){let B=F.isScene===!0?F.overrideMaterial:null;for(let V=0,fe=x.length;V<fe;V++){let _e=x[V],{object:de,geometry:be,group:we}=_e,qe=_e.material;qe.allowOverride===!0&&B!==null&&(qe=B),de.layers.test(z.layers)&&f_(de,F,z,be,qe,we)}}function f_(x,F,z,B,V,fe){x.onBeforeRender(O,F,z,B,V,fe),x.modelViewMatrix.multiplyMatrices(z.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),V.onBeforeRender(O,F,z,B,x,fe),V.transparent===!0&&V.side===fi&&V.forceSinglePass===!1?(V.side=Pn,V.needsUpdate=!0,O.renderBufferDirect(z,F,B,V,x,fe),V.side=Rr,V.needsUpdate=!0,O.renderBufferDirect(z,F,B,V,x,fe),V.side=fi):O.renderBufferDirect(z,F,B,V,x,fe),x.onAfterRender(O,F,z,B,V,fe)}function Ul(x,F,z){F.isScene!==!0&&(F=Kt);let B=E.get(x),V=w.state.lights,fe=w.state.shadowsArray,_e=V.state.version,de=oe.getParameters(x,V.state,fe,F,z,w.state.lightProbeGridArray),be=oe.getProgramCacheKey(de),we=B.programs;B.environment=x.isMeshStandardMaterial||x.isMeshLambertMaterial||x.isMeshPhongMaterial?F.environment:null,B.fog=F.fog;let qe=x.isMeshStandardMaterial||x.isMeshLambertMaterial&&!x.envMap||x.isMeshPhongMaterial&&!x.envMap;B.envMap=k.get(x.envMap||B.environment,qe),B.envMapRotation=B.environment!==null&&x.envMap===null?F.environmentRotation:x.envMapRotation,we===void 0&&(x.addEventListener("dispose",$t),we=new Map,B.programs=we);let Qe=we.get(be);if(Qe!==void 0){if(B.currentProgram===Qe&&B.lightsStateVersion===_e)return p_(x,de),Qe}else de.uniforms=oe.getUniforms(x),L!==null&&x.isNodeMaterial&&L.build(x,z,de),x.onBeforeCompile(de,O),Qe=oe.acquireProgram(de,be),we.set(be,Qe),B.uniforms=de.uniforms;let De=B.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(De.clippingPlanes=ye.uniform),p_(x,de),B.needsLights=QM(x),B.lightsStateVersion=_e,B.needsLights&&(De.ambientLightColor.value=V.state.ambient,De.lightProbe.value=V.state.probe,De.directionalLights.value=V.state.directional,De.directionalLightShadows.value=V.state.directionalShadow,De.spotLights.value=V.state.spot,De.spotLightShadows.value=V.state.spotShadow,De.rectAreaLights.value=V.state.rectArea,De.ltc_1.value=V.state.rectAreaLTC1,De.ltc_2.value=V.state.rectAreaLTC2,De.pointLights.value=V.state.point,De.pointLightShadows.value=V.state.pointShadow,De.hemisphereLights.value=V.state.hemi,De.directionalShadowMatrix.value=V.state.directionalShadowMatrix,De.spotLightMatrix.value=V.state.spotLightMatrix,De.spotLightMap.value=V.state.spotLightMap,De.pointShadowMatrix.value=V.state.pointShadowMatrix),B.lightProbeGrid=w.state.lightProbeGridArray.length>0,B.currentProgram=Qe,B.uniformsList=null,Qe}function h_(x){if(x.uniformsList===null){let F=x.currentProgram.getUniforms();x.uniformsList=za.seqWithValue(F.seq,x.uniforms)}return x.uniformsList}function p_(x,F){let z=E.get(x);z.outputColorSpace=F.outputColorSpace,z.batching=F.batching,z.batchingColor=F.batchingColor,z.instancing=F.instancing,z.instancingColor=F.instancingColor,z.instancingMorph=F.instancingMorph,z.skinning=F.skinning,z.morphTargets=F.morphTargets,z.morphNormals=F.morphNormals,z.morphColors=F.morphColors,z.morphTargetsCount=F.morphTargetsCount,z.numClippingPlanes=F.numClippingPlanes,z.numIntersection=F.numClipIntersection,z.vertexAlphas=F.vertexAlphas,z.vertexTangents=F.vertexTangents,z.toneMapping=F.toneMapping}function ZM(x,F){if(x.length===0)return null;if(x.length===1)return x[0].texture!==null?x[0]:null;S.setFromMatrixPosition(F.matrixWorld);for(let z=0,B=x.length;z<B;z++){let V=x[z];if(V.texture!==null&&V.boundingBox.containsPoint(S))return V}return null}function KM(x,F,z,B,V){F.isScene!==!0&&(F=Kt),v.resetTextureUnits();let fe=F.fog,_e=B.isMeshStandardMaterial||B.isMeshLambertMaterial||B.isMeshPhongMaterial?F.environment:null,de=P===null?O.outputColorSpace:P.isXRRenderTarget===!0?P.texture.colorSpace:at.workingColorSpace,be=B.isMeshStandardMaterial||B.isMeshLambertMaterial&&!B.envMap||B.isMeshPhongMaterial&&!B.envMap,we=k.get(B.envMap||_e,be),qe=B.vertexColors===!0&&!!z.attributes.color&&z.attributes.color.itemSize===4,Qe=!!z.attributes.tangent&&(!!B.normalMap||B.anisotropy>0),De=!!z.morphAttributes.position,_t=!!z.morphAttributes.normal,qt=!!z.morphAttributes.color,Ht=Ai;B.toneMapped&&(P===null||P.isXRRenderTarget===!0)&&(Ht=O.toneMapping);let Mt=z.morphAttributes.position||z.morphAttributes.normal||z.morphAttributes.color,yn=Mt!==void 0?Mt.length:0,ve=E.get(B),Wn=w.state.lights;if(Lt===!0&&(st===!0||x!==U)){let It=x===U&&B.id===H;ye.setState(B,x,It)}let lt=!1;B.version===ve.__version?(ve.needsLights&&ve.lightsStateVersion!==Wn.state.version||ve.outputColorSpace!==de||V.isBatchedMesh&&ve.batching===!1||!V.isBatchedMesh&&ve.batching===!0||V.isBatchedMesh&&ve.batchingColor===!0&&V.colorTexture===null||V.isBatchedMesh&&ve.batchingColor===!1&&V.colorTexture!==null||V.isInstancedMesh&&ve.instancing===!1||!V.isInstancedMesh&&ve.instancing===!0||V.isSkinnedMesh&&ve.skinning===!1||!V.isSkinnedMesh&&ve.skinning===!0||V.isInstancedMesh&&ve.instancingColor===!0&&V.instanceColor===null||V.isInstancedMesh&&ve.instancingColor===!1&&V.instanceColor!==null||V.isInstancedMesh&&ve.instancingMorph===!0&&V.morphTexture===null||V.isInstancedMesh&&ve.instancingMorph===!1&&V.morphTexture!==null||ve.envMap!==we||B.fog===!0&&ve.fog!==fe||ve.numClippingPlanes!==void 0&&(ve.numClippingPlanes!==ye.numPlanes||ve.numIntersection!==ye.numIntersection)||ve.vertexAlphas!==qe||ve.vertexTangents!==Qe||ve.morphTargets!==De||ve.morphNormals!==_t||ve.morphColors!==qt||ve.toneMapping!==Ht||ve.morphTargetsCount!==yn||!!ve.lightProbeGrid!=w.state.lightProbeGridArray.length>0)&&(lt=!0):(lt=!0,ve.__version=B.version);let ni=ve.currentProgram;lt===!0&&(ni=Ul(B,F,V),L&&B.isNodeMaterial&&L.onUpdateProgram(B,ni,ve));let Oi=!1,Or=!1,fo=!1,wt=ni.getUniforms(),Xt=ve.uniforms;if(ue.useProgram(ni.program)&&(Oi=!0,Or=!0,fo=!0),B.id!==H&&(H=B.id,Or=!0),ve.needsLights){let It=ZM(w.state.lightProbeGridArray,V);ve.lightProbeGrid!==It&&(ve.lightProbeGrid=It,Or=!0)}if(Oi||U!==x){ue.buffers.depth.getReversed()&&x.reversedDepth!==!0&&(x._reversedDepth=!0,x.updateProjectionMatrix()),wt.setValue(N,"projectionMatrix",x.projectionMatrix),wt.setValue(N,"viewMatrix",x.matrixWorldInverse);let Lr=wt.map.cameraPosition;Lr!==void 0&&Lr.setValue(N,Ot.setFromMatrixPosition(x.matrixWorld)),At.logarithmicDepthBuffer&&wt.setValue(N,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(B.isMeshPhongMaterial||B.isMeshToonMaterial||B.isMeshLambertMaterial||B.isMeshBasicMaterial||B.isMeshStandardMaterial||B.isShaderMaterial)&&wt.setValue(N,"isOrthographic",x.isOrthographicCamera===!0),U!==x&&(U=x,Or=!0,fo=!0)}if(ve.needsLights&&(Wn.state.directionalShadowMap.length>0&&wt.setValue(N,"directionalShadowMap",Wn.state.directionalShadowMap,v),Wn.state.spotShadowMap.length>0&&wt.setValue(N,"spotShadowMap",Wn.state.spotShadowMap,v),Wn.state.pointShadowMap.length>0&&wt.setValue(N,"pointShadowMap",Wn.state.pointShadowMap,v)),V.isSkinnedMesh){wt.setOptional(N,V,"bindMatrix"),wt.setOptional(N,V,"bindMatrixInverse");let It=V.skeleton;It&&(It.boneTexture===null&&It.computeBoneTexture(),wt.setValue(N,"boneTexture",It.boneTexture,v))}V.isBatchedMesh&&(wt.setOptional(N,V,"batchingTexture"),wt.setValue(N,"batchingTexture",V._matricesTexture,v),wt.setOptional(N,V,"batchingIdTexture"),wt.setValue(N,"batchingIdTexture",V._indirectTexture,v),wt.setOptional(N,V,"batchingColorTexture"),V._colorsTexture!==null&&wt.setValue(N,"batchingColorTexture",V._colorsTexture,v));let Fr=z.morphAttributes;if((Fr.position!==void 0||Fr.normal!==void 0||Fr.color!==void 0)&&He.update(V,z,ni),(Or||ve.receiveShadow!==V.receiveShadow)&&(ve.receiveShadow=V.receiveShadow,wt.setValue(N,"receiveShadow",V.receiveShadow)),(B.isMeshStandardMaterial||B.isMeshLambertMaterial||B.isMeshPhongMaterial)&&B.envMap===null&&F.environment!==null&&(Xt.envMapIntensity.value=F.environmentIntensity),Xt.dfgLUT!==void 0&&(Xt.dfgLUT.value=c2()),Or){if(wt.setValue(N,"toneMappingExposure",O.toneMappingExposure),ve.needsLights&&JM(Xt,fo),fe&&B.fog===!0&&j.refreshFogUniforms(Xt,fe),j.refreshMaterialUniforms(Xt,B,Fe,Ke,w.state.transmissionRenderTarget[x.id]),ve.needsLights&&ve.lightProbeGrid){let It=ve.lightProbeGrid;Xt.probesSH.value=It.texture,Xt.probesMin.value.copy(It.boundingBox.min),Xt.probesMax.value.copy(It.boundingBox.max),Xt.probesResolution.value.copy(It.resolution)}za.upload(N,h_(ve),Xt,v)}if(B.isShaderMaterial&&B.uniformsNeedUpdate===!0&&(za.upload(N,h_(ve),Xt,v),B.uniformsNeedUpdate=!1),B.isSpriteMaterial&&wt.setValue(N,"center",V.center),wt.setValue(N,"modelViewMatrix",V.modelViewMatrix),wt.setValue(N,"normalMatrix",V.normalMatrix),wt.setValue(N,"modelMatrix",V.matrixWorld),B.uniformsGroups!==void 0){let It=B.uniformsGroups;for(let Lr=0,ho=It.length;Lr<ho;Lr++){let m_=It[Lr];$.update(m_,ni),$.bind(m_,ni)}}return ni}function JM(x,F){x.ambientLightColor.needsUpdate=F,x.lightProbe.needsUpdate=F,x.directionalLights.needsUpdate=F,x.directionalLightShadows.needsUpdate=F,x.pointLights.needsUpdate=F,x.pointLightShadows.needsUpdate=F,x.spotLights.needsUpdate=F,x.spotLightShadows.needsUpdate=F,x.rectAreaLights.needsUpdate=F,x.hemisphereLights.needsUpdate=F}function QM(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return G},this.getActiveMipmapLevel=function(){return W},this.getRenderTarget=function(){return P},this.setRenderTargetTextures=function(x,F,z){let B=E.get(x);B.__autoAllocateDepthBuffer=x.resolveDepthBuffer===!1,B.__autoAllocateDepthBuffer===!1&&(B.__useRenderToTexture=!1),E.get(x.texture).__webglTexture=F,E.get(x.depthTexture).__webglTexture=B.__autoAllocateDepthBuffer?void 0:z,B.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(x,F){let z=E.get(x);z.__webglFramebuffer=F,z.__useDefaultFramebuffer=F===void 0};let ew=N.createFramebuffer();this.setRenderTarget=function(x,F=0,z=0){P=x,G=F,W=z;let B=null,V=!1,fe=!1;if(x){let de=E.get(x);if(de.__useDefaultFramebuffer!==void 0){ue.bindFramebuffer(N.FRAMEBUFFER,de.__webglFramebuffer),J.copy(x.viewport),Q.copy(x.scissor),ce=x.scissorTest,ue.viewport(J),ue.scissor(Q),ue.setScissorTest(ce),H=-1;return}else if(de.__webglFramebuffer===void 0)v.setupRenderTarget(x);else if(de.__hasExternalTextures)v.rebindTextures(x,E.get(x.texture).__webglTexture,E.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let qe=x.depthTexture;if(de.__boundDepthTexture!==qe){if(qe!==null&&E.has(qe)&&(x.width!==qe.image.width||x.height!==qe.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");v.setupDepthRenderbuffer(x)}}let be=x.texture;(be.isData3DTexture||be.isDataArrayTexture||be.isCompressedArrayTexture)&&(fe=!0);let we=E.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(we[F])?B=we[F][z]:B=we[F],V=!0):x.samples>0&&v.useMultisampledRTT(x)===!1?B=E.get(x).__webglMultisampledFramebuffer:Array.isArray(we)?B=we[z]:B=we,J.copy(x.viewport),Q.copy(x.scissor),ce=x.scissorTest}else J.copy(ee).multiplyScalar(Fe).floor(),Q.copy(Oe).multiplyScalar(Fe).floor(),ce=We;if(z!==0&&(B=ew),ue.bindFramebuffer(N.FRAMEBUFFER,B)&&ue.drawBuffers(x,B),ue.viewport(J),ue.scissor(Q),ue.setScissorTest(ce),V){let de=E.get(x.texture);N.framebufferTexture2D(N.FRAMEBUFFER,N.COLOR_ATTACHMENT0,N.TEXTURE_CUBE_MAP_POSITIVE_X+F,de.__webglTexture,z)}else if(fe){let de=F;for(let be=0;be<x.textures.length;be++){let we=E.get(x.textures[be]);N.framebufferTextureLayer(N.FRAMEBUFFER,N.COLOR_ATTACHMENT0+be,we.__webglTexture,z,de)}}else if(x!==null&&z!==0){let de=E.get(x.texture);N.framebufferTexture2D(N.FRAMEBUFFER,N.COLOR_ATTACHMENT0,N.TEXTURE_2D,de.__webglTexture,z)}H=-1},this.readRenderTargetPixels=function(x,F,z,B,V,fe,_e,de=0){if(!(x&&x.isWebGLRenderTarget)){Pe("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let be=E.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&_e!==void 0&&(be=be[_e]),be){ue.bindFramebuffer(N.FRAMEBUFFER,be);try{let we=x.textures[de],qe=we.format,Qe=we.type;if(x.textures.length>1&&N.readBuffer(N.COLOR_ATTACHMENT0+de),!At.textureFormatReadable(qe)){Pe("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!At.textureTypeReadable(Qe)){Pe("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}F>=0&&F<=x.width-B&&z>=0&&z<=x.height-V&&N.readPixels(F,z,B,V,R.convert(qe),R.convert(Qe),fe)}finally{let we=P!==null?E.get(P).__webglFramebuffer:null;ue.bindFramebuffer(N.FRAMEBUFFER,we)}}},this.readRenderTargetPixelsAsync=function(x,F,z,B,V,fe,_e,de=0){return cn(this,null,function*(){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let be=E.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&_e!==void 0&&(be=be[_e]),be)if(F>=0&&F<=x.width-B&&z>=0&&z<=x.height-V){ue.bindFramebuffer(N.FRAMEBUFFER,be);let we=x.textures[de],qe=we.format,Qe=we.type;if(x.textures.length>1&&N.readBuffer(N.COLOR_ATTACHMENT0+de),!At.textureFormatReadable(qe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!At.textureTypeReadable(Qe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let De=N.createBuffer();N.bindBuffer(N.PIXEL_PACK_BUFFER,De),N.bufferData(N.PIXEL_PACK_BUFFER,fe.byteLength,N.STREAM_READ),N.readPixels(F,z,B,V,R.convert(qe),R.convert(Qe),0);let _t=P!==null?E.get(P).__webglFramebuffer:null;ue.bindFramebuffer(N.FRAMEBUFFER,_t);let qt=N.fenceSync(N.SYNC_GPU_COMMANDS_COMPLETE,0);return N.flush(),yield iM(N,qt,4),N.bindBuffer(N.PIXEL_PACK_BUFFER,De),N.getBufferSubData(N.PIXEL_PACK_BUFFER,0,fe),N.deleteBuffer(De),N.deleteSync(qt),fe}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")})},this.copyFramebufferToTexture=function(x,F=null,z=0){let B=Math.pow(2,-z),V=Math.floor(x.image.width*B),fe=Math.floor(x.image.height*B),_e=F!==null?F.x:0,de=F!==null?F.y:0;v.setTexture2D(x,0),N.copyTexSubImage2D(N.TEXTURE_2D,z,0,0,_e,de,V,fe),ue.unbindTexture()};let tw=N.createFramebuffer(),nw=N.createFramebuffer();this.copyTextureToTexture=function(x,F,z=null,B=null,V=0,fe=0){let _e,de,be,we,qe,Qe,De,_t,qt,Ht=x.isCompressedTexture?x.mipmaps[fe]:x.image;if(z!==null)_e=z.max.x-z.min.x,de=z.max.y-z.min.y,be=z.isBox3?z.max.z-z.min.z:1,we=z.min.x,qe=z.min.y,Qe=z.isBox3?z.min.z:0;else{let Xt=Math.pow(2,-V);_e=Math.floor(Ht.width*Xt),de=Math.floor(Ht.height*Xt),x.isDataArrayTexture?be=Ht.depth:x.isData3DTexture?be=Math.floor(Ht.depth*Xt):be=1,we=0,qe=0,Qe=0}B!==null?(De=B.x,_t=B.y,qt=B.z):(De=0,_t=0,qt=0);let Mt=R.convert(F.format),yn=R.convert(F.type),ve;F.isData3DTexture?(v.setTexture3D(F,0),ve=N.TEXTURE_3D):F.isDataArrayTexture||F.isCompressedArrayTexture?(v.setTexture2DArray(F,0),ve=N.TEXTURE_2D_ARRAY):(v.setTexture2D(F,0),ve=N.TEXTURE_2D),ue.activeTexture(N.TEXTURE0),ue.pixelStorei(N.UNPACK_FLIP_Y_WEBGL,F.flipY),ue.pixelStorei(N.UNPACK_PREMULTIPLY_ALPHA_WEBGL,F.premultiplyAlpha),ue.pixelStorei(N.UNPACK_ALIGNMENT,F.unpackAlignment);let Wn=ue.getParameter(N.UNPACK_ROW_LENGTH),lt=ue.getParameter(N.UNPACK_IMAGE_HEIGHT),ni=ue.getParameter(N.UNPACK_SKIP_PIXELS),Oi=ue.getParameter(N.UNPACK_SKIP_ROWS),Or=ue.getParameter(N.UNPACK_SKIP_IMAGES);ue.pixelStorei(N.UNPACK_ROW_LENGTH,Ht.width),ue.pixelStorei(N.UNPACK_IMAGE_HEIGHT,Ht.height),ue.pixelStorei(N.UNPACK_SKIP_PIXELS,we),ue.pixelStorei(N.UNPACK_SKIP_ROWS,qe),ue.pixelStorei(N.UNPACK_SKIP_IMAGES,Qe);let fo=x.isDataArrayTexture||x.isData3DTexture,wt=F.isDataArrayTexture||F.isData3DTexture;if(x.isDepthTexture){let Xt=E.get(x),Fr=E.get(F),It=E.get(Xt.__renderTarget),Lr=E.get(Fr.__renderTarget);ue.bindFramebuffer(N.READ_FRAMEBUFFER,It.__webglFramebuffer),ue.bindFramebuffer(N.DRAW_FRAMEBUFFER,Lr.__webglFramebuffer);for(let ho=0;ho<be;ho++)fo&&(N.framebufferTextureLayer(N.READ_FRAMEBUFFER,N.COLOR_ATTACHMENT0,E.get(x).__webglTexture,V,Qe+ho),N.framebufferTextureLayer(N.DRAW_FRAMEBUFFER,N.COLOR_ATTACHMENT0,E.get(F).__webglTexture,fe,qt+ho)),N.blitFramebuffer(we,qe,_e,de,De,_t,_e,de,N.DEPTH_BUFFER_BIT,N.NEAREST);ue.bindFramebuffer(N.READ_FRAMEBUFFER,null),ue.bindFramebuffer(N.DRAW_FRAMEBUFFER,null)}else if(V!==0||x.isRenderTargetTexture||E.has(x)){let Xt=E.get(x),Fr=E.get(F);ue.bindFramebuffer(N.READ_FRAMEBUFFER,tw),ue.bindFramebuffer(N.DRAW_FRAMEBUFFER,nw);for(let It=0;It<be;It++)fo?N.framebufferTextureLayer(N.READ_FRAMEBUFFER,N.COLOR_ATTACHMENT0,Xt.__webglTexture,V,Qe+It):N.framebufferTexture2D(N.READ_FRAMEBUFFER,N.COLOR_ATTACHMENT0,N.TEXTURE_2D,Xt.__webglTexture,V),wt?N.framebufferTextureLayer(N.DRAW_FRAMEBUFFER,N.COLOR_ATTACHMENT0,Fr.__webglTexture,fe,qt+It):N.framebufferTexture2D(N.DRAW_FRAMEBUFFER,N.COLOR_ATTACHMENT0,N.TEXTURE_2D,Fr.__webglTexture,fe),V!==0?N.blitFramebuffer(we,qe,_e,de,De,_t,_e,de,N.COLOR_BUFFER_BIT,N.NEAREST):wt?N.copyTexSubImage3D(ve,fe,De,_t,qt+It,we,qe,_e,de):N.copyTexSubImage2D(ve,fe,De,_t,we,qe,_e,de);ue.bindFramebuffer(N.READ_FRAMEBUFFER,null),ue.bindFramebuffer(N.DRAW_FRAMEBUFFER,null)}else wt?x.isDataTexture||x.isData3DTexture?N.texSubImage3D(ve,fe,De,_t,qt,_e,de,be,Mt,yn,Ht.data):F.isCompressedArrayTexture?N.compressedTexSubImage3D(ve,fe,De,_t,qt,_e,de,be,Mt,Ht.data):N.texSubImage3D(ve,fe,De,_t,qt,_e,de,be,Mt,yn,Ht):x.isDataTexture?N.texSubImage2D(N.TEXTURE_2D,fe,De,_t,_e,de,Mt,yn,Ht.data):x.isCompressedTexture?N.compressedTexSubImage2D(N.TEXTURE_2D,fe,De,_t,Ht.width,Ht.height,Mt,Ht.data):N.texSubImage2D(N.TEXTURE_2D,fe,De,_t,_e,de,Mt,yn,Ht);ue.pixelStorei(N.UNPACK_ROW_LENGTH,Wn),ue.pixelStorei(N.UNPACK_IMAGE_HEIGHT,lt),ue.pixelStorei(N.UNPACK_SKIP_PIXELS,ni),ue.pixelStorei(N.UNPACK_SKIP_ROWS,Oi),ue.pixelStorei(N.UNPACK_SKIP_IMAGES,Or),fe===0&&F.generateMipmaps&&N.generateMipmap(ve),ue.unbindTexture()},this.initRenderTarget=function(x){E.get(x).__webglFramebuffer===void 0&&v.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?v.setTextureCube(x,0):x.isData3DTexture?v.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?v.setTexture2DArray(x,0):v.setTexture2D(x,0),ue.unbindTexture()},this.resetState=function(){G=0,W=0,P=null,ue.reset(),ie.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return Ti}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=at._getDrawingBufferColorSpace(e),t.unpackColorSpace=at._getUnpackColorSpace()}};var LM={type:"change"},r_={type:"start"},UM={type:"end"},lp=new ls,kM=new Vn,u2=Math.cos(70*Es.DEG2RAD),sn=new T,Gn=2*Math.PI,St={NONE:-1,ROTATE:0,DOLLY:1,PAN:2,TOUCH_ROTATE:3,TOUCH_PAN:4,TOUCH_DOLLY_PAN:5,TOUCH_DOLLY_ROTATE:6},i_=1e-6,up=class extends El{constructor(e,t=null){super(e,t),this.state=St.NONE,this.target=new T,this.cursor=new T,this.minDistance=0,this.maxDistance=1/0,this.minZoom=0,this.maxZoom=1/0,this.minTargetRadius=0,this.maxTargetRadius=1/0,this.minPolarAngle=0,this.maxPolarAngle=Math.PI,this.minAzimuthAngle=-1/0,this.maxAzimuthAngle=1/0,this.enableDamping=!1,this.dampingFactor=.05,this.enableZoom=!0,this.zoomSpeed=1,this.enableRotate=!0,this.rotateSpeed=1,this.keyRotateSpeed=1,this.enablePan=!0,this.panSpeed=1,this.screenSpacePanning=!0,this.keyPanSpeed=7,this.zoomToCursor=!1,this.autoRotate=!1,this.autoRotateSpeed=2,this.keys={LEFT:"ArrowLeft",UP:"ArrowUp",RIGHT:"ArrowRight",BOTTOM:"ArrowDown"},this.mouseButtons={LEFT:gs.ROTATE,MIDDLE:gs.DOLLY,RIGHT:gs.PAN},this.touches={ONE:vs.ROTATE,TWO:vs.DOLLY_PAN},this.target0=this.target.clone(),this.position0=this.object.position.clone(),this.zoom0=this.object.zoom,this._cursorStyle="auto",this._domElementKeyEvents=null,this._lastPosition=new T,this._lastQuaternion=new pn,this._lastTargetPosition=new T,this._quat=new pn().setFromUnitVectors(e.up,new T(0,1,0)),this._quatInverse=this._quat.clone().invert(),this._spherical=new ka,this._sphericalDelta=new ka,this._scale=1,this._panOffset=new T,this._rotateStart=new Te,this._rotateEnd=new Te,this._rotateDelta=new Te,this._panStart=new Te,this._panEnd=new Te,this._panDelta=new Te,this._dollyStart=new Te,this._dollyEnd=new Te,this._dollyDelta=new Te,this._dollyDirection=new T,this._mouse=new Te,this._performCursorZoom=!1,this._pointers=[],this._pointerPositions={},this._controlActive=!1,this._onPointerMove=f2.bind(this),this._onPointerDown=d2.bind(this),this._onPointerUp=h2.bind(this),this._onContextMenu=x2.bind(this),this._onMouseWheel=g2.bind(this),this._onKeyDown=v2.bind(this),this._onTouchStart=y2.bind(this),this._onTouchMove=_2.bind(this),this._onMouseDown=p2.bind(this),this._onMouseMove=m2.bind(this),this._interceptControlDown=b2.bind(this),this._interceptControlUp=E2.bind(this),this.domElement!==null&&this.connect(this.domElement),this.update()}set cursorStyle(e){this._cursorStyle=e,e==="grab"?this.domElement.style.cursor="grab":this.domElement.style.cursor="auto"}get cursorStyle(){return this._cursorStyle}connect(e){super.connect(e),this.domElement.addEventListener("pointerdown",this._onPointerDown),this.domElement.addEventListener("pointercancel",this._onPointerUp),this.domElement.addEventListener("contextmenu",this._onContextMenu),this.domElement.addEventListener("wheel",this._onMouseWheel,{passive:!1}),this.domElement.getRootNode().addEventListener("keydown",this._interceptControlDown,{passive:!0,capture:!0}),this.domElement.style.touchAction="none"}disconnect(){this.domElement.removeEventListener("pointerdown",this._onPointerDown),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.domElement.removeEventListener("pointercancel",this._onPointerUp),this.domElement.removeEventListener("wheel",this._onMouseWheel),this.domElement.removeEventListener("contextmenu",this._onContextMenu),this.stopListenToKeyEvents(),this.domElement.getRootNode().removeEventListener("keydown",this._interceptControlDown,{capture:!0}),this.domElement.style.touchAction=""}dispose(){this.disconnect()}getPolarAngle(){return this._spherical.phi}getAzimuthalAngle(){return this._spherical.theta}getDistance(){return this.object.position.distanceTo(this.target)}listenToKeyEvents(e){e.addEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=e}stopListenToKeyEvents(){this._domElementKeyEvents!==null&&(this._domElementKeyEvents.removeEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=null)}saveState(){this.target0.copy(this.target),this.position0.copy(this.object.position),this.zoom0=this.object.zoom}reset(){this.target.copy(this.target0),this.object.position.copy(this.position0),this.object.zoom=this.zoom0,this.object.updateProjectionMatrix(),this.dispatchEvent(LM),this.update(),this.state=St.NONE}pan(e,t){this._pan(e,t),this.update()}dollyIn(e){this._dollyIn(e),this.update()}dollyOut(e){this._dollyOut(e),this.update()}rotateLeft(e){this._rotateLeft(e),this.update()}rotateUp(e){this._rotateUp(e),this.update()}update(e=null){let t=this.object.position;sn.copy(t).sub(this.target),sn.applyQuaternion(this._quat),this._spherical.setFromVector3(sn),this.autoRotate&&this.state===St.NONE&&this._rotateLeft(this._getAutoRotationAngle(e)),this.enableDamping?(this._spherical.theta+=this._sphericalDelta.theta*this.dampingFactor,this._spherical.phi+=this._sphericalDelta.phi*this.dampingFactor):(this._spherical.theta+=this._sphericalDelta.theta,this._spherical.phi+=this._sphericalDelta.phi);let i=this.minAzimuthAngle,r=this.maxAzimuthAngle;isFinite(i)&&isFinite(r)&&(i<-Math.PI?i+=Gn:i>Math.PI&&(i-=Gn),r<-Math.PI?r+=Gn:r>Math.PI&&(r-=Gn),i<=r?this._spherical.theta=Math.max(i,Math.min(r,this._spherical.theta)):this._spherical.theta=this._spherical.theta>(i+r)/2?Math.max(i,this._spherical.theta):Math.min(r,this._spherical.theta)),this._spherical.phi=Math.max(this.minPolarAngle,Math.min(this.maxPolarAngle,this._spherical.phi)),this._spherical.makeSafe(),this.enableDamping===!0?this.target.addScaledVector(this._panOffset,this.dampingFactor):this.target.add(this._panOffset),this.target.sub(this.cursor),this.target.clampLength(this.minTargetRadius,this.maxTargetRadius),this.target.add(this.cursor);let s=!1;if(this.zoomToCursor&&this._performCursorZoom||this.object.isOrthographicCamera)this._spherical.radius=this._clampDistance(this._spherical.radius);else{let o=this._spherical.radius;this._spherical.radius=this._clampDistance(this._spherical.radius*this._scale),s=o!=this._spherical.radius}if(sn.setFromSpherical(this._spherical),sn.applyQuaternion(this._quatInverse),t.copy(this.target).add(sn),this.object.lookAt(this.target),this.enableDamping===!0?(this._sphericalDelta.theta*=1-this.dampingFactor,this._sphericalDelta.phi*=1-this.dampingFactor,this._panOffset.multiplyScalar(1-this.dampingFactor)):(this._sphericalDelta.set(0,0,0),this._panOffset.set(0,0,0)),this.zoomToCursor&&this._performCursorZoom){let o=null;if(this.object.isPerspectiveCamera){let a=sn.length();o=this._clampDistance(a*this._scale);let c=a-o;this.object.position.addScaledVector(this._dollyDirection,c),this.object.updateMatrixWorld(),s=!!c}else if(this.object.isOrthographicCamera){let a=new T(this._mouse.x,this._mouse.y,0);a.unproject(this.object);let c=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),this.object.updateProjectionMatrix(),s=c!==this.object.zoom;let l=new T(this._mouse.x,this._mouse.y,0);l.unproject(this.object),this.object.position.sub(l).add(a),this.object.updateMatrixWorld(),o=sn.length()}else console.warn("WARNING: OrbitControls.js encountered an unknown camera type - zoom to cursor disabled."),this.zoomToCursor=!1;o!==null&&(this.screenSpacePanning?this.target.set(0,0,-1).transformDirection(this.object.matrix).multiplyScalar(o).add(this.object.position):(lp.origin.copy(this.object.position),lp.direction.set(0,0,-1).transformDirection(this.object.matrix),Math.abs(this.object.up.dot(lp.direction))<u2?this.object.lookAt(this.target):(kM.setFromNormalAndCoplanarPoint(this.object.up,this.target),lp.intersectPlane(kM,this.target))))}else if(this.object.isOrthographicCamera){let o=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),o!==this.object.zoom&&(this.object.updateProjectionMatrix(),s=!0)}return this._scale=1,this._performCursorZoom=!1,s||this._lastPosition.distanceToSquared(this.object.position)>i_||8*(1-this._lastQuaternion.dot(this.object.quaternion))>i_||this._lastTargetPosition.distanceToSquared(this.target)>i_?(this.dispatchEvent(LM),this._lastPosition.copy(this.object.position),this._lastQuaternion.copy(this.object.quaternion),this._lastTargetPosition.copy(this.target),!0):!1}_getAutoRotationAngle(e){return e!==null?Gn/60*this.autoRotateSpeed*e:Gn/60/60*this.autoRotateSpeed}_getZoomScale(e){let t=Math.abs(e*.01);return Math.pow(.95,this.zoomSpeed*t)}_rotateLeft(e){this._sphericalDelta.theta-=e}_rotateUp(e){this._sphericalDelta.phi-=e}_panLeft(e,t){sn.setFromMatrixColumn(t,0),sn.multiplyScalar(-e),this._panOffset.add(sn)}_panUp(e,t){this.screenSpacePanning===!0?sn.setFromMatrixColumn(t,1):(sn.setFromMatrixColumn(t,0),sn.crossVectors(this.object.up,sn)),sn.multiplyScalar(e),this._panOffset.add(sn)}_pan(e,t){let i=this.domElement;if(this.object.isPerspectiveCamera){let r=this.object.position;sn.copy(r).sub(this.target);let s=sn.length();s*=Math.tan(this.object.fov/2*Math.PI/180),this._panLeft(2*e*s/i.clientHeight,this.object.matrix),this._panUp(2*t*s/i.clientHeight,this.object.matrix)}else this.object.isOrthographicCamera?(this._panLeft(e*(this.object.right-this.object.left)/this.object.zoom/i.clientWidth,this.object.matrix),this._panUp(t*(this.object.top-this.object.bottom)/this.object.zoom/i.clientHeight,this.object.matrix)):(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - pan disabled."),this.enablePan=!1)}_dollyOut(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale/=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_dollyIn(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale*=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_updateZoomParameters(e,t){if(!this.zoomToCursor)return;this._performCursorZoom=!0;let i=this.domElement.getBoundingClientRect(),r=e-i.left,s=t-i.top,o=i.width,a=i.height;this._mouse.x=r/o*2-1,this._mouse.y=-(s/a)*2+1,this._dollyDirection.set(this._mouse.x,this._mouse.y,1).unproject(this.object).sub(this.object.position).normalize()}_clampDistance(e){return Math.max(this.minDistance,Math.min(this.maxDistance,e))}_handleMouseDownRotate(e){this._rotateStart.set(e.clientX,e.clientY)}_handleMouseDownDolly(e){this._updateZoomParameters(e.clientX,e.clientX),this._dollyStart.set(e.clientX,e.clientY)}_handleMouseDownPan(e){this._panStart.set(e.clientX,e.clientY)}_handleMouseMoveRotate(e){this._rotateEnd.set(e.clientX,e.clientY),this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(Gn*this._rotateDelta.x/t.clientHeight),this._rotateUp(Gn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd),this.update()}_handleMouseMoveDolly(e){this._dollyEnd.set(e.clientX,e.clientY),this._dollyDelta.subVectors(this._dollyEnd,this._dollyStart),this._dollyDelta.y>0?this._dollyOut(this._getZoomScale(this._dollyDelta.y)):this._dollyDelta.y<0&&this._dollyIn(this._getZoomScale(this._dollyDelta.y)),this._dollyStart.copy(this._dollyEnd),this.update()}_handleMouseMovePan(e){this._panEnd.set(e.clientX,e.clientY),this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd),this.update()}_handleMouseWheel(e){this._updateZoomParameters(e.clientX,e.clientY),e.deltaY<0?this._dollyIn(this._getZoomScale(e.deltaY)):e.deltaY>0&&this._dollyOut(this._getZoomScale(e.deltaY)),this.update()}_handleKeyDown(e){let t=!1;switch(e.code){case this.keys.UP:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(Gn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,this.keyPanSpeed),t=!0;break;case this.keys.BOTTOM:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(-Gn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,-this.keyPanSpeed),t=!0;break;case this.keys.LEFT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(Gn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(this.keyPanSpeed,0),t=!0;break;case this.keys.RIGHT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(-Gn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(-this.keyPanSpeed,0),t=!0;break}t&&(e.preventDefault(),this.update())}_handleTouchStartRotate(e){if(this._pointers.length===1)this._rotateStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._rotateStart.set(i,r)}}_handleTouchStartPan(e){if(this._pointers.length===1)this._panStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panStart.set(i,r)}}_handleTouchStartDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyStart.set(0,s)}_handleTouchStartDollyPan(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enablePan&&this._handleTouchStartPan(e)}_handleTouchStartDollyRotate(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enableRotate&&this._handleTouchStartRotate(e)}_handleTouchMoveRotate(e){if(this._pointers.length==1)this._rotateEnd.set(e.pageX,e.pageY);else{let i=this._getSecondPointerPosition(e),r=.5*(e.pageX+i.x),s=.5*(e.pageY+i.y);this._rotateEnd.set(r,s)}this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(Gn*this._rotateDelta.x/t.clientHeight),this._rotateUp(Gn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd)}_handleTouchMovePan(e){if(this._pointers.length===1)this._panEnd.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panEnd.set(i,r)}this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd)}_handleTouchMoveDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyEnd.set(0,s),this._dollyDelta.set(0,Math.pow(this._dollyEnd.y/this._dollyStart.y,this.zoomSpeed)),this._dollyOut(this._dollyDelta.y),this._dollyStart.copy(this._dollyEnd);let o=(e.pageX+t.x)*.5,a=(e.pageY+t.y)*.5;this._updateZoomParameters(o,a)}_handleTouchMoveDollyPan(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enablePan&&this._handleTouchMovePan(e)}_handleTouchMoveDollyRotate(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enableRotate&&this._handleTouchMoveRotate(e)}_addPointer(e){this._pointers.push(e.pointerId)}_removePointer(e){delete this._pointerPositions[e.pointerId];for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId){this._pointers.splice(t,1);return}}_isTrackingPointer(e){for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId)return!0;return!1}_trackPointer(e){let t=this._pointerPositions[e.pointerId];t===void 0&&(t=new Te,this._pointerPositions[e.pointerId]=t),t.set(e.pageX,e.pageY)}_getSecondPointerPosition(e){let t=e.pointerId===this._pointers[0]?this._pointers[1]:this._pointers[0];return this._pointerPositions[t]}_customWheelEvent(e){let t=e.deltaMode,i={clientX:e.clientX,clientY:e.clientY,deltaY:e.deltaY};switch(t){case 1:i.deltaY*=16;break;case 2:i.deltaY*=100;break}return e.ctrlKey&&!this._controlActive&&(i.deltaY*=10),i}};function d2(n){this.enabled!==!1&&(this._pointers.length===0&&(this.domElement.setPointerCapture(n.pointerId),this.domElement.ownerDocument.addEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.addEventListener("pointerup",this._onPointerUp)),!this._isTrackingPointer(n)&&(this._addPointer(n),n.pointerType==="touch"?this._onTouchStart(n):this._onMouseDown(n),this._cursorStyle==="grab"&&(this.domElement.style.cursor="grabbing")))}function f2(n){this.enabled!==!1&&(n.pointerType==="touch"?this._onTouchMove(n):this._onMouseMove(n))}function h2(n){switch(this._removePointer(n),this._pointers.length){case 0:this.domElement.releasePointerCapture(n.pointerId),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.dispatchEvent(UM),this.state=St.NONE,this._cursorStyle==="grab"&&(this.domElement.style.cursor="grab");break;case 1:let e=this._pointers[0],t=this._pointerPositions[e];this._onTouchStart({pointerId:e,pageX:t.x,pageY:t.y});break}}function p2(n){let e;switch(n.button){case 0:e=this.mouseButtons.LEFT;break;case 1:e=this.mouseButtons.MIDDLE;break;case 2:e=this.mouseButtons.RIGHT;break;default:e=-1}switch(e){case gs.DOLLY:if(this.enableZoom===!1)return;this._handleMouseDownDolly(n),this.state=St.DOLLY;break;case gs.ROTATE:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=St.PAN}else{if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=St.ROTATE}break;case gs.PAN:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=St.ROTATE}else{if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=St.PAN}break;default:this.state=St.NONE}this.state!==St.NONE&&this.dispatchEvent(r_)}function m2(n){switch(this.state){case St.ROTATE:if(this.enableRotate===!1)return;this._handleMouseMoveRotate(n);break;case St.DOLLY:if(this.enableZoom===!1)return;this._handleMouseMoveDolly(n);break;case St.PAN:if(this.enablePan===!1)return;this._handleMouseMovePan(n);break}}function g2(n){this.enabled===!1||this.enableZoom===!1||this.state!==St.NONE||(n.preventDefault(),this.dispatchEvent(r_),this._handleMouseWheel(this._customWheelEvent(n)),this.dispatchEvent(UM))}function v2(n){this.enabled!==!1&&this._handleKeyDown(n)}function y2(n){switch(this._trackPointer(n),this._pointers.length){case 1:switch(this.touches.ONE){case vs.ROTATE:if(this.enableRotate===!1)return;this._handleTouchStartRotate(n),this.state=St.TOUCH_ROTATE;break;case vs.PAN:if(this.enablePan===!1)return;this._handleTouchStartPan(n),this.state=St.TOUCH_PAN;break;default:this.state=St.NONE}break;case 2:switch(this.touches.TWO){case vs.DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchStartDollyPan(n),this.state=St.TOUCH_DOLLY_PAN;break;case vs.DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchStartDollyRotate(n),this.state=St.TOUCH_DOLLY_ROTATE;break;default:this.state=St.NONE}break;default:this.state=St.NONE}this.state!==St.NONE&&this.dispatchEvent(r_)}function _2(n){switch(this._trackPointer(n),this.state){case St.TOUCH_ROTATE:if(this.enableRotate===!1)return;this._handleTouchMoveRotate(n),this.update();break;case St.TOUCH_PAN:if(this.enablePan===!1)return;this._handleTouchMovePan(n),this.update();break;case St.TOUCH_DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchMoveDollyPan(n),this.update();break;case St.TOUCH_DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchMoveDollyRotate(n),this.update();break;default:this.state=St.NONE}}function x2(n){this.enabled!==!1&&n.preventDefault()}function b2(n){n.key==="Control"&&(this._controlActive=!0,this.domElement.getRootNode().addEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}function E2(n){n.key==="Control"&&(this._controlActive=!1,this.domElement.getRootNode().removeEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}function BM(n,e,t){let i=t.length-n-1;if(e>=t[i])return i-1;if(e<=t[n])return n;let r=n,s=i,o=Math.floor((r+s)/2);for(;e<t[o]||e>=t[o+1];)e<t[o]?s=o:r=o,o=Math.floor((r+s)/2);return o}function VM(n,e,t,i){let r=[],s=[],o=[];r[0]=1;for(let a=1;a<=t;++a){s[a]=e-i[n+1-a],o[a]=i[n+a]-e;let c=0;for(let l=0;l<a;++l){let u=o[l+1],d=s[a-l],f=r[l]/(u+d);r[l]=c+u*f,c=d*f}r[a]=c}return r}function HM(n,e,t,i,r,s,o,a){let c=BM(n,s,t),l=BM(e,o,i),u=VM(c,s,n,t),d=VM(l,o,e,i),f=[];for(let g=0;g<=e;++g){f[g]=new it(0,0,0,0);for(let y=0;y<=n;++y){let p=r[c-n+y][l-e+g].clone(),m=p.w;p.x*=m,p.y*=m,p.z*=m,f[g].add(p.multiplyScalar(u[y]))}}let h=new it(0,0,0,0);for(let g=0;g<=e;++g)h.add(f[g].multiplyScalar(d[g]));h.divideScalar(h.w),a.set(h.x,h.y,h.z)}var Ll=class{constructor(e,t,i,r,s){this.degree1=e,this.degree2=t,this.knots1=i,this.knots2=r,this.controlPoints=[];let o=i.length-e-1,a=r.length-t-1;for(let c=0;c<o;++c){this.controlPoints[c]=[];for(let l=0;l<a;++l){let u=s[c][l];this.controlPoints[c][l]=new it(u.x,u.y,u.z,u.w)}}}getPoint(e,t,i){let r=this.knots1[0]+e*(this.knots1[this.knots1.length-1]-this.knots1[0]),s=this.knots2[0]+t*(this.knots2[this.knots2.length-1]-this.knots2[0]);HM(this.degree1,this.degree2,this.knots1,this.knots2,this.controlPoints,r,s,i)}};var dp=class extends Wt{constructor(e=(r,s,o)=>o.set(r,s,Math.cos(r)*Math.sin(s)),t=8,i=8){super(),this.type="ParametricGeometry",this.parameters={func:e,slices:t,stacks:i};let r=[],s=[],o=[],a=[],c=1e-5,l=new T,u=new T,d=new T,f=new T,h=new T,g=t+1;for(let y=0;y<=i;y++){let p=y/i;for(let m=0;m<=t;m++){let b=m/t;e(b,p,u),s.push(u.x,u.y,u.z),b-c>=0?(e(b-c,p,d),f.subVectors(u,d)):(e(b+c,p,d),f.subVectors(d,u)),p-c>=0?(e(b,p-c,d),h.subVectors(u,d)):(e(b,p+c,d),h.subVectors(d,u)),l.crossVectors(f,h).normalize(),o.push(l.x,l.y,l.z),a.push(b,p)}}for(let y=0;y<i;y++)for(let p=0;p<t;p++){let m=y*g+p,b=y*g+p+1,M=(y+1)*g+p+1,S=(y+1)*g+p;r.push(m,b,S),r.push(b,M,S)}this.setIndex(r),this.setAttribute("position",new ut(s,3)),this.setAttribute("normal",new ut(o,3)),this.setAttribute("uv",new ut(a,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}};var zM=new Dn,fp=new T,ja=class extends vl{constructor(){super(),this.isLineSegmentsGeometry=!0,this.type="LineSegmentsGeometry";let e=[-1,2,0,1,2,0,-1,1,0,1,1,0,-1,0,0,1,0,0,-1,-1,0,1,-1,0],t=[-1,2,1,2,-1,1,1,1,-1,-1,1,-1,-1,-2,1,-2],i=[0,2,1,2,3,1,2,4,3,4,5,3,4,6,5,6,7,5];this.setIndex(i),this.setAttribute("position",new ut(e,3)),this.setAttribute("uv",new ut(t,2))}applyMatrix4(e){let t=this.attributes.instanceStart,i=this.attributes.instanceEnd;return t!==void 0&&(t.applyMatrix4(e),i.applyMatrix4(e),t.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}setPositions(e){let t;e instanceof Float32Array?t=e:Array.isArray(e)&&(t=new Float32Array(e));let i=new ms(t,6,1);return this.setAttribute("instanceStart",new ei(i,3,0)),this.setAttribute("instanceEnd",new ei(i,3,3)),this.instanceCount=this.attributes.instanceStart.count,this.computeBoundingBox(),this.computeBoundingSphere(),this}setColors(e){let t;e instanceof Float32Array?t=e:Array.isArray(e)&&(t=new Float32Array(e));let i=new ms(t,6,1);return this.setAttribute("instanceColorStart",new ei(i,3,0)),this.setAttribute("instanceColorEnd",new ei(i,3,3)),this}fromWireframeGeometry(e){return this.setPositions(e.attributes.position.array),this}fromEdgesGeometry(e){return this.setPositions(e.attributes.position.array),this}fromMesh(e){return this.fromWireframeGeometry(new fl(e.geometry)),this}fromLineSegments(e){let t=e.geometry;return this.setPositions(t.attributes.position.array),this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Dn);let e=this.attributes.instanceStart,t=this.attributes.instanceEnd;e!==void 0&&t!==void 0&&(this.boundingBox.setFromBufferAttribute(e),zM.setFromBufferAttribute(t),this.boundingBox.union(zM))}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Qn),this.boundingBox===null&&this.computeBoundingBox();let e=this.attributes.instanceStart,t=this.attributes.instanceEnd;if(e!==void 0&&t!==void 0){let i=this.boundingSphere.center;this.boundingBox.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)fp.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(fp)),fp.fromBufferAttribute(t,s),r=Math.max(r,i.distanceToSquared(fp));this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&console.error("THREE.LineSegmentsGeometry.computeBoundingSphere(): Computed radius is NaN. The instanced position data is likely to have NaN values.",this)}}toJSON(){}};ae.line={worldUnits:{value:1},linewidth:{value:1},resolution:{value:new Te(1,1)},dashOffset:{value:0},dashScale:{value:1},dashSize:{value:1},gapSize:{value:1}};On.line={uniforms:Nl.merge([ae.common,ae.fog,ae.line]),vertexShader:`
		#include <common>
		#include <color_pars_vertex>
		#include <fog_pars_vertex>
		#include <logdepthbuf_pars_vertex>
		#include <clipping_planes_pars_vertex>

		uniform float linewidth;
		uniform vec2 resolution;

		attribute vec3 instanceStart;
		attribute vec3 instanceEnd;

		attribute vec3 instanceColorStart;
		attribute vec3 instanceColorEnd;

		#ifdef WORLD_UNITS

			varying vec4 worldPos;
			varying vec3 worldStart;
			varying vec3 worldEnd;

			#ifdef USE_DASH

				varying vec2 vUv;

			#endif

		#else

			varying vec2 vUv;

		#endif

		#ifdef USE_DASH

			uniform float dashScale;
			attribute float instanceDistanceStart;
			attribute float instanceDistanceEnd;
			varying float vLineDistance;

		#endif

		void trimSegment( const in vec4 start, inout vec4 end ) {

			// trim end segment so it terminates between the camera plane and the near plane

			// conservative estimate of the near plane
			float a = projectionMatrix[ 2 ][ 2 ]; // 3nd entry in 3th column
			float b = projectionMatrix[ 3 ][ 2 ]; // 3nd entry in 4th column
			float nearEstimate = - 0.5 * b / a;

			float alpha = ( nearEstimate - start.z ) / ( end.z - start.z );

			end.xyz = mix( start.xyz, end.xyz, alpha );

		}

		void main() {

			#ifdef USE_COLOR

				vColor.xyz = ( position.y < 0.5 ) ? instanceColorStart : instanceColorEnd;

			#endif

			#ifdef USE_DASH

				vLineDistance = ( position.y < 0.5 ) ? dashScale * instanceDistanceStart : dashScale * instanceDistanceEnd;
				vUv = uv;

			#endif

			float aspect = resolution.x / resolution.y;

			// camera space
			vec4 start = modelViewMatrix * vec4( instanceStart, 1.0 );
			vec4 end = modelViewMatrix * vec4( instanceEnd, 1.0 );

			#ifdef WORLD_UNITS

				worldStart = start.xyz;
				worldEnd = end.xyz;

			#else

				vUv = uv;

			#endif

			// special case for perspective projection, and segments that terminate either in, or behind, the camera plane
			// clearly the gpu firmware has a way of addressing this issue when projecting into ndc space
			// but we need to perform ndc-space calculations in the shader, so we must address this issue directly
			// perhaps there is a more elegant solution -- WestLangley

			bool perspective = ( projectionMatrix[ 2 ][ 3 ] == - 1.0 ); // 4th entry in the 3rd column

			if ( perspective ) {

				if ( start.z < 0.0 && end.z >= 0.0 ) {

					trimSegment( start, end );

				} else if ( end.z < 0.0 && start.z >= 0.0 ) {

					trimSegment( end, start );

				}

			}

			// clip space
			vec4 clipStart = projectionMatrix * start;
			vec4 clipEnd = projectionMatrix * end;

			// ndc space
			vec3 ndcStart = clipStart.xyz / clipStart.w;
			vec3 ndcEnd = clipEnd.xyz / clipEnd.w;

			// direction
			vec2 dir = ndcEnd.xy - ndcStart.xy;

			// account for clip-space aspect ratio
			dir.x *= aspect;
			dir = normalize( dir );

			#ifdef WORLD_UNITS

				vec3 worldDir = normalize( end.xyz - start.xyz );
				vec3 tmpFwd = normalize( mix( start.xyz, end.xyz, 0.5 ) );
				vec3 worldUp = normalize( cross( worldDir, tmpFwd ) );
				vec3 worldFwd = cross( worldDir, worldUp );
				worldPos = position.y < 0.5 ? start: end;

				// height offset
				float hw = linewidth * 0.5;
				worldPos.xyz += position.x < 0.0 ? hw * worldUp : - hw * worldUp;

				// don't extend the line if we're rendering dashes because we
				// won't be rendering the endcaps
				#ifndef USE_DASH

					// cap extension
					worldPos.xyz += position.y < 0.5 ? - hw * worldDir : hw * worldDir;

					// add width to the box
					worldPos.xyz += worldFwd * hw;

					// endcaps
					if ( position.y > 1.0 || position.y < 0.0 ) {

						worldPos.xyz -= worldFwd * 2.0 * hw;

					}

				#endif

				// project the worldpos
				vec4 clip = projectionMatrix * worldPos;

				// shift the depth of the projected points so the line
				// segments overlap neatly
				vec3 clipPose = ( position.y < 0.5 ) ? ndcStart : ndcEnd;
				clip.z = clipPose.z * clip.w;

			#else

				vec2 offset = vec2( dir.y, - dir.x );
				// undo aspect ratio adjustment
				dir.x /= aspect;
				offset.x /= aspect;

				// sign flip
				if ( position.x < 0.0 ) offset *= - 1.0;

				// endcaps
				if ( position.y < 0.0 ) {

					offset += - dir;

				} else if ( position.y > 1.0 ) {

					offset += dir;

				}

				// adjust for linewidth
				offset *= linewidth;

				// adjust for clip-space to screen-space conversion // maybe resolution should be based on viewport ...
				offset /= resolution.y;

				// select end
				vec4 clip = ( position.y < 0.5 ) ? clipStart : clipEnd;

				// back to clip space
				offset *= clip.w;

				clip.xy += offset;

			#endif

			gl_Position = clip;

			vec4 mvPosition = ( position.y < 0.5 ) ? start : end; // this is an approximation

			#include <logdepthbuf_vertex>
			#include <clipping_planes_vertex>
			#include <fog_vertex>

		}
		`,fragmentShader:`
		uniform vec3 diffuse;
		uniform float opacity;
		uniform float linewidth;

		#ifdef USE_DASH

			uniform float dashOffset;
			uniform float dashSize;
			uniform float gapSize;

		#endif

		varying float vLineDistance;

		#ifdef WORLD_UNITS

			varying vec4 worldPos;
			varying vec3 worldStart;
			varying vec3 worldEnd;

			#ifdef USE_DASH

				varying vec2 vUv;

			#endif

		#else

			varying vec2 vUv;

		#endif

		#include <common>
		#include <color_pars_fragment>
		#include <fog_pars_fragment>
		#include <logdepthbuf_pars_fragment>
		#include <clipping_planes_pars_fragment>

		vec2 closestLineToLine(vec3 p1, vec3 p2, vec3 p3, vec3 p4) {

			float mua;
			float mub;

			vec3 p13 = p1 - p3;
			vec3 p43 = p4 - p3;

			vec3 p21 = p2 - p1;

			float d1343 = dot( p13, p43 );
			float d4321 = dot( p43, p21 );
			float d1321 = dot( p13, p21 );
			float d4343 = dot( p43, p43 );
			float d2121 = dot( p21, p21 );

			float denom = d2121 * d4343 - d4321 * d4321;

			float numer = d1343 * d4321 - d1321 * d4343;

			mua = numer / denom;
			mua = clamp( mua, 0.0, 1.0 );
			mub = ( d1343 + d4321 * ( mua ) ) / d4343;
			mub = clamp( mub, 0.0, 1.0 );

			return vec2( mua, mub );

		}

		void main() {

			float alpha = opacity;
			vec4 diffuseColor = vec4( diffuse, alpha );

			#include <clipping_planes_fragment>

			#ifdef USE_DASH

				if ( vUv.y < - 1.0 || vUv.y > 1.0 ) discard; // discard endcaps

				if ( mod( vLineDistance + dashOffset, dashSize + gapSize ) > dashSize ) discard; // todo - FIX

			#endif

			#ifdef WORLD_UNITS

				// Find the closest points on the view ray and the line segment
				vec3 rayEnd = normalize( worldPos.xyz ) * 1e5;
				vec3 lineDir = worldEnd - worldStart;
				vec2 params = closestLineToLine( worldStart, worldEnd, vec3( 0.0, 0.0, 0.0 ), rayEnd );

				vec3 p1 = worldStart + lineDir * params.x;
				vec3 p2 = rayEnd * params.y;
				vec3 delta = p1 - p2;
				float len = length( delta );
				float norm = len / linewidth;

				#ifndef USE_DASH

					#ifdef USE_ALPHA_TO_COVERAGE

						float dnorm = fwidth( norm );
						alpha = 1.0 - smoothstep( 0.5 - dnorm, 0.5 + dnorm, norm );

					#else

						if ( norm > 0.5 ) {

							discard;

						}

					#endif

				#endif

			#else

				#ifdef USE_ALPHA_TO_COVERAGE

					// artifacts appear on some hardware if a derivative is taken within a conditional
					float a = vUv.x;
					float b = ( vUv.y > 0.0 ) ? vUv.y - 1.0 : vUv.y + 1.0;
					float len2 = a * a + b * b;
					float dlen = fwidth( len2 );

					if ( abs( vUv.y ) > 1.0 ) {

						alpha = 1.0 - smoothstep( 1.0 - dlen, 1.0 + dlen, len2 );

					}

				#else

					if ( abs( vUv.y ) > 1.0 ) {

						float a = vUv.x;
						float b = ( vUv.y > 0.0 ) ? vUv.y - 1.0 : vUv.y + 1.0;
						float len2 = a * a + b * b;

						if ( len2 > 1.0 ) discard;

					}

				#endif

			#endif

			#include <logdepthbuf_fragment>
			#include <color_fragment>

			gl_FragColor = vec4( diffuseColor.rgb, alpha );

			#include <tonemapping_fragment>
			#include <colorspace_fragment>
			#include <fog_fragment>
			#include <premultiplied_alpha_fragment>

		}
		`};var Ms=class extends Nn{constructor(e){super({type:"LineMaterial",uniforms:Nl.clone(On.line.uniforms),vertexShader:On.line.vertexShader,fragmentShader:On.line.fragmentShader,clipping:!0}),this.isLineMaterial=!0,this.setValues(e)}get color(){return this.uniforms.diffuse.value}set color(e){this.uniforms.diffuse.value=e}get worldUnits(){return"WORLD_UNITS"in this.defines}set worldUnits(e){e===!0!==this.worldUnits&&(this.needsUpdate=!0),e===!0?this.defines.WORLD_UNITS="":delete this.defines.WORLD_UNITS}get linewidth(){return this.uniforms.linewidth.value}set linewidth(e){this.uniforms.linewidth&&(this.uniforms.linewidth.value=e)}get dashed(){return"USE_DASH"in this.defines}set dashed(e){e===!0!==this.dashed&&(this.needsUpdate=!0),e===!0?this.defines.USE_DASH="":delete this.defines.USE_DASH}get dashScale(){return this.uniforms.dashScale.value}set dashScale(e){this.uniforms.dashScale.value=e}get dashSize(){return this.uniforms.dashSize.value}set dashSize(e){this.uniforms.dashSize.value=e}get dashOffset(){return this.uniforms.dashOffset.value}set dashOffset(e){this.uniforms.dashOffset.value=e}get gapSize(){return this.uniforms.gapSize.value}set gapSize(e){this.uniforms.gapSize.value=e}get opacity(){return this.uniforms.opacity.value}set opacity(e){this.uniforms&&(this.uniforms.opacity.value=e)}get resolution(){return this.uniforms.resolution.value}set resolution(e){this.uniforms.resolution.value.copy(e)}get alphaToCoverage(){return"USE_ALPHA_TO_COVERAGE"in this.defines}set alphaToCoverage(e){this.defines&&(e===!0!==this.alphaToCoverage&&(this.needsUpdate=!0),e===!0?this.defines.USE_ALPHA_TO_COVERAGE="":delete this.defines.USE_ALPHA_TO_COVERAGE)}};var s_=new it,GM=new T,jM=new T,mn=new it,gn=new it,sr=new it,o_=new T,a_=new Dt,vn=new _l,WM=new T,hp=new Dn,pp=new Qn,or=new it,ar,uo;function $M(n,e,t){return or.set(0,0,-e,1).applyMatrix4(n.projectionMatrix),or.multiplyScalar(1/or.w),or.x=uo/t.width,or.y=uo/t.height,or.applyMatrix4(n.projectionMatrixInverse),or.multiplyScalar(1/or.w),Math.abs(Math.max(or.x,or.y))}function M2(n,e){let t=n.matrixWorld,i=n.geometry,r=i.attributes.instanceStart,s=i.attributes.instanceEnd,o=Math.min(i.instanceCount,r.count);for(let a=0,c=o;a<c;a++){vn.start.fromBufferAttribute(r,a),vn.end.fromBufferAttribute(s,a),vn.applyMatrix4(t);let l=new T,u=new T;ar.distanceSqToSegment(vn.start,vn.end,u,l),u.distanceTo(l)<uo*.5&&e.push({point:u,pointOnLine:l,distance:ar.origin.distanceTo(u),object:n,face:null,faceIndex:a,uv:null,uv1:null})}}function w2(n,e,t){let i=e.projectionMatrix,s=n.material.resolution,o=n.matrixWorld,a=n.geometry,c=a.attributes.instanceStart,l=a.attributes.instanceEnd,u=Math.min(a.instanceCount,c.count),d=-e.near;ar.at(1,sr),sr.w=1,sr.applyMatrix4(e.matrixWorldInverse),sr.applyMatrix4(i),sr.multiplyScalar(1/sr.w),sr.x*=s.x/2,sr.y*=s.y/2,sr.z=0,o_.copy(sr),a_.multiplyMatrices(e.matrixWorldInverse,o);for(let f=0,h=u;f<h;f++){if(mn.fromBufferAttribute(c,f),gn.fromBufferAttribute(l,f),mn.w=1,gn.w=1,mn.applyMatrix4(a_),gn.applyMatrix4(a_),mn.z>d&&gn.z>d)continue;if(mn.z>d){let M=mn.z-gn.z,S=(mn.z-d)/M;mn.lerp(gn,S)}else if(gn.z>d){let M=gn.z-mn.z,S=(gn.z-d)/M;gn.lerp(mn,S)}mn.applyMatrix4(i),gn.applyMatrix4(i),mn.multiplyScalar(1/mn.w),gn.multiplyScalar(1/gn.w),mn.x*=s.x/2,mn.y*=s.y/2,gn.x*=s.x/2,gn.y*=s.y/2,vn.start.copy(mn),vn.start.z=0,vn.end.copy(gn),vn.end.z=0;let y=vn.closestPointToPointParameter(o_,!0);vn.at(y,WM);let p=Es.lerp(mn.z,gn.z,y),m=p>=-1&&p<=1,b=o_.distanceTo(WM)<uo*.5;if(m&&b){vn.start.fromBufferAttribute(c,f),vn.end.fromBufferAttribute(l,f),vn.start.applyMatrix4(o),vn.end.applyMatrix4(o);let M=new T,S=new T;ar.distanceSqToSegment(vn.start,vn.end,S,M),t.push({point:S,pointOnLine:M,distance:ar.origin.distanceTo(S),object:n,face:null,faceIndex:f,uv:null,uv1:null})}}}var mp=class extends tn{constructor(e=new ja,t=new Ms({color:Math.random()*16777215})){super(e,t),this.isLineSegments2=!0,this.type="LineSegments2"}computeLineDistances(){let e=this.geometry,t=e.attributes.instanceStart,i=e.attributes.instanceEnd,r=new Float32Array(2*t.count);for(let o=0,a=0,c=t.count;o<c;o++,a+=2)GM.fromBufferAttribute(t,o),jM.fromBufferAttribute(i,o),r[a]=a===0?0:r[a-1],r[a+1]=r[a]+GM.distanceTo(jM);let s=new ms(r,2,1);return e.setAttribute("instanceDistanceStart",new ei(s,1,0)),e.setAttribute("instanceDistanceEnd",new ei(s,1,1)),this}raycast(e,t){let i=this.material.worldUnits,r=e.camera;r===null&&!i&&console.error('LineSegments2: "Raycaster.camera" needs to be set in order to raycast against LineSegments2 while worldUnits is set to false.');let s=e.params.Line2!==void 0&&e.params.Line2.threshold||0;ar=e.ray;let o=this.matrixWorld,a=this.geometry,c=this.material;uo=c.linewidth+s,a.boundingSphere===null&&a.computeBoundingSphere(),pp.copy(a.boundingSphere).applyMatrix4(o);let l;if(i)l=uo*.5;else{let d=Math.max(r.near,pp.distanceToPoint(ar.origin));l=$M(r,d,c.resolution)}if(pp.radius+=l,ar.intersectsSphere(pp)===!1)return;a.boundingBox===null&&a.computeBoundingBox(),hp.copy(a.boundingBox).applyMatrix4(o);let u;if(i)u=uo*.5;else{let d=Math.max(r.near,hp.distanceToPoint(ar.origin));u=$M(r,d,c.resolution)}hp.expandByScalar(u),ar.intersectsBox(hp)!==!1&&(i?M2(this,t):w2(this,r,t))}onBeforeRender(e){let t=this.material.uniforms;t&&t.resolution&&(e.getViewport(s_),this.material.uniforms.resolution.value.set(s_.z,s_.w))}};var Wa=class extends ja{constructor(){super(),this.isLineGeometry=!0,this.type="LineGeometry"}setPositions(e){let t=e.length-3,i=new Float32Array(2*t);for(let r=0;r<t;r+=3)i[2*r]=e[r],i[2*r+1]=e[r+1],i[2*r+2]=e[r+2],i[2*r+3]=e[r+3],i[2*r+4]=e[r+4],i[2*r+5]=e[r+5];return super.setPositions(i),this}setColors(e){let t=e.length-3,i=new Float32Array(2*t);for(let r=0;r<t;r+=3)i[2*r]=e[r],i[2*r+1]=e[r+1],i[2*r+2]=e[r+2],i[2*r+3]=e[r+3],i[2*r+4]=e[r+4],i[2*r+5]=e[r+5];return super.setColors(i),this}setFromPoints(e){let t=e.length-1,i=new Float32Array(6*t);for(let r=0;r<t;r++)i[6*r]=e[r].x,i[6*r+1]=e[r].y,i[6*r+2]=e[r].z||0,i[6*r+3]=e[r+1].x,i[6*r+4]=e[r+1].y,i[6*r+5]=e[r+1].z||0;return super.setPositions(i),this}fromLine(e){let t=e.geometry;return this.setPositions(t.attributes.position.array),this}};var gp=class extends mp{constructor(e=new Wa,t=new Ms({color:Math.random()*16777215})){super(e,t),this.isLine2=!0,this.type="Line2"}};var C2=["canvasHost"],T2=(n,e)=>e.id;function D2(n,e){if(n&1&&(Oo(),En(0,"text",16),Ee(1),Sn()),n&2){let t=nt().$implicit;xn("x",t.labelX)("y",t.labelY),Ce(),fn(t.label)}}function A2(n,e){if(n&1){let t=br();Oo(),En(0,"path",15),xi("click",function(){let r=Ct(t).$implicit,s=nt();return Tt(s.setCameraDirection(r.direction.x,r.direction.y,r.direction.z))}),En(1,"title"),Ee(2),Sn()(),Un(3,D2,2,3,":svg:text",16)}if(n&2){let t=e.$implicit;dn("cube-chamfer",t.kind==="chamfer"),xn("d",t.path)("fill",t.color),Ce(2),Er("",t.label," view"),Ce(),Bn(t.kind==="face"?3:-1)}}var Ni={initialPosition:new T(120,90,120),initialTarget:new T(0,0,0),upDirection:new T(0,1,0),framingDirection:new T(1,.7,1).normalize(),framingPadding:1.2,maxDistanceMultiplier:20,nearDistanceRatio:.001,farDistanceRatio:1e3},qM="patchId",XM="cpKey",vp=class n{constructor(e,t,i){this.selectionService=e;this.ngZone=t;this.changeDetectorRef=i}selectionService;ngZone;changeDetectorRef;canvasHost;patches=[];selectedId=null;hoveredId=null;selectedPatch=null;patchHover=new xt;patchSelect=new xt;patchDragComplete=new xt;patchesLoaded=new xt;exportStep=new xt;scene;camera;renderer;controls;raycaster=new yl;pointer=new Te;animationFrameId=0;resizeObserver;patchMeshes=new Map;tempColor=new Ve;cpGroup=new di;netGroup=new di;curveGroup=new di;axisTriad=new di;axisTriadReferenceDepth=120;viewCubeFaces=[];curveLines=new Map;cpSpheres=new Map;sharedSphereGeometry=null;dragState=null;wasDragging=!1;subscription=new Ft;hoveredCpKey=null;clickedCpKey=null;curveAnimationActive=!1;curveAnimationStartTime=0;curveAnimationDuration=1400;cameraAnimation=null;cameraAnimationDuration=320;needsReframe=!0;ngAfterViewInit(){console.log("Initializing Three.js scene..."),this.initScene(),console.log("Scene initialized. Rendering initial frame..."),this.rebuildMeshes(),console.log("Initial frame rendered. Starting animation loop..."),this.ngZone.runOutsideAngular(()=>this.animate()),this.bindEvents(),this.setupControlPointHoverSubscription()}setupControlPointHoverSubscription(){this.subscription.add(this.selectionService.hoveredCp$.subscribe(e=>{if(e!==this.hoveredCpKey&&(this.clearCpHover(),this.hoveredCpKey=e,e)){let t=this.cpSpheres.get(e);t&&this.applyMaterialColor(t,16711680,.55)}}))}ngOnChanges(e){if(console.log("ngOnChanges called on",performance.now()),!!this.scene){if(console.log("ViewportComponent detected input changes:",e),e.patches){let t=e.patches.previousValue;(!t||t.length===0)&&(this.needsReframe=!0),this.rebuildMeshes(),console.log(`Meshes rebuilt. Total patches: ${this.patches.length}`);return}(e.selectedId||e.selectedPatch)&&(this.clickedCpKey=null),(e.selectedId||e.hoveredId)&&this.updateHighlights(),(e.selectedPatch||e.selectedId)&&this.rebuildControlPointVisualization()}}ngOnDestroy(){this.subscription.unsubscribe(),cancelAnimationFrame(this.animationFrameId),this.resizeObserver?.disconnect(),this.controls?.dispose(),this.renderer?.dispose(),this.disposeMeshes(),this.disposeControlPoints()}initScene(){let e=this.canvasHost.nativeElement;this.scene=new nl,this.scene.background=new Ve(725024),this.camera=new Tn(45,1,.1,1e4),this.camera.position.copy(Ni.initialPosition),this.camera.up.set(0,1,0),this.renderer=new op({antialias:!0,alpha:!0}),this.renderer.setPixelRatio(Math.min(window.devicePixelRatio,2)),this.renderer.outputColorSpace=Cn,e.appendChild(this.renderer.domElement),this.controls=new up(this.camera,this.renderer.domElement),this.controls.enableDamping=!0,this.controls.dampingFactor=.08,this.controls.addEventListener("change",()=>this.updateViewCube());let t=new gl(16777215,.55),i=new La(16777215,1.1);i.position.set(80,120,60);let r=new La(9090303,.45);r.position.set(-60,40,-80),this.scene.add(t,i,r);let s=new xl(200,20,3359846,1712696);s.position.y=-.01,this.scene.add(s),this.createAxisTriad(),this.scene.add(this.axisTriad),this.scene.add(this.cpGroup,this.netGroup,this.curveGroup),this.resizeObserver=new ResizeObserver(()=>this.resize()),this.resizeObserver.observe(e),this.resize(),this.updateViewCube(),window.addEventListener("keydown",this.handleKeyDown.bind(this))}handleKeyDown(e){let t=e.target;t.tagName==="INPUT"||t.tagName==="TEXTAREA"||e.key.toLowerCase()==="f"&&this.frameCameraToPatches()}bindEvents(){let e=this.renderer.domElement;e.addEventListener("pointerdown",t=>{if(t.button!==0)return;this.updatePointer(t);let i=this.pickControlPoint();if(i){let r=`${i.row},${i.col}`;this.clickedCpKey=r,this.startDrag(i.sphere,i.row,i.col),this.wasDragging=!0,e.setPointerCapture(t.pointerId)}else this.clickedCpKey=null,this.selectionService.hoverCp(null)}),e.addEventListener("pointermove",t=>{if(this.updatePointer(t),this.dragState){this.handleDragMove(),e.style.cursor="grabbing";return}let i=this.pickControlPoint();if(i){let s=`${i.row},${i.col}`;s!==this.hoveredCpKey&&this.selectionService.hoverCp(s),e.style.cursor="grab";return}this.hoveredCpKey!==this.clickedCpKey&&this.selectionService.hoverCp(this.clickedCpKey),e.style.cursor="";let r=this.pickPatchId();r!==this.hoveredId&&(this.patchHover.emit(r),this.selectionService.hover(r))}),e.addEventListener("pointerup",()=>{this.dragState&&(this.endDrag(),e.style.cursor="")}),e.addEventListener("pointerleave",()=>{this.dragState&&(this.endDrag(),e.style.cursor=""),this.selectionService.hoverCp(this.clickedCpKey),this.patchHover.emit(null),this.selectionService.hover(null)}),e.addEventListener("click",()=>{if(this.wasDragging){this.wasDragging=!1;return}let t=this.pickPatchId();this.patchSelect.emit(t),this.selectionService.select(t)})}updatePointer(e){let t=this.renderer.domElement.getBoundingClientRect();this.pointer.x=(e.clientX-t.left)/t.width*2-1,this.pointer.y=-((e.clientY-t.top)/t.height)*2+1}pickControlPoint(){if(this.cpSpheres.size===0)return null;this.raycaster.setFromCamera(this.pointer,this.camera);let e=Array.from(this.cpSpheres.values()),t=this.raycaster.intersectObjects(e,!1);if(!t.length)return null;let i=t[0].object,r=i.userData[XM],[s,o]=r.split(",").map(Number);return{sphere:i,row:s,col:o}}pickPatchId(){this.raycaster.setFromCamera(this.pointer,this.camera);let e=Array.from(this.patchMeshes.values()),i=this.raycaster.intersectObjects(e,!1)[0];return i?i.object.userData[qM]??null:null}startDrag(e,t,i){if(!this.selectedPatch)return;let r=new T;e.getWorldPosition(r);let s=new T;this.camera.getWorldDirection(s);let o=new Vn().setFromNormalAndCoplanarPoint(s,r);this.dragState={patchId:this.selectedPatch.id,row:t,col:i,sphere:e,dragPlane:o,workingPatch:this.clonePatch(this.selectedPatch)},this.applyMaterialColor(e,16729344,.8),this.controls.enabled=!1}handleDragMove(){if(!this.dragState)return;this.raycaster.setFromCamera(this.pointer,this.camera);let e=new T;if(!this.raycaster.ray.intersectPlane(this.dragState.dragPlane,e))return;let{row:t,col:i,sphere:r}=this.dragState,s=this.dragState.workingPatch,o=s.controlPoints.map((a,c)=>c===t?a.map((l,u)=>u===i?ke(Ae({},l),{x:e.x,y:e.y,z:e.z}):l):a);this.dragState.workingPatch=ke(Ae({},s),{controlPoints:o}),r.position.copy(e),this.retessellatePatch(this.dragState.workingPatch),this.rebuildNetLines(this.dragState.workingPatch),this.rebuildCurveVisualization(this.dragState.workingPatch)}endDrag(){if(!this.dragState)return;this.applyMaterialColor(this.dragState.sphere,16766720,.3),this.patchDragComplete.emit(this.dragState.workingPatch),this.dragState=null,this.controls.enabled=!0;let e=this.pickControlPoint();e?this.selectionService.hoverCp(`${e.row},${e.col}`):this.selectionService.hoverCp(null)}retessellatePatch(e){let t=this.patchMeshes.get(e.id);if(!t)return;let i=t.geometry;t.geometry=this.buildGeometry(e),i.dispose()}buildGeometry(e){let t=e.controlPoints.map(a=>a.map(c=>new it(c.x,c.y,c.z,c.w))),i=new Ll(e.degreeU,e.degreeV,e.knotsU,e.knotsV,t),r=Math.max(16,e.degreeU*6),s=Math.max(16,e.degreeV*6),o=new dp((a,c,l)=>{i.getPoint(a,c,l)},r,s);return o.computeVertexNormals(),o}rebuildControlPointVisualization(){let e=this.selectionService.hoveredCp;if(this.disposeControlPoints(),!this.selectedPatch||!this.selectedId)return;let t=this.selectedPatch,i=this.computeCpRadius(t);this.sharedSphereGeometry=new Pa(i,12,8);for(let r=0;r<t.controlPoints.length;r++)for(let s=0;s<t.controlPoints[r].length;s++){let o=t.controlPoints[r][s],a=new Oa({color:16766720,emissive:new Ve(4469504),emissiveIntensity:.3,metalness:.15,roughness:.25}),c=new tn(this.sharedSphereGeometry,a);c.position.set(o.x,o.y,o.z),c.userData[XM]=`${r},${s}`,c.renderOrder=1,this.cpGroup.add(c),this.cpSpheres.set(`${r},${s}`,c)}if(e){this.hoveredCpKey=e;let r=this.cpSpheres.get(e);r&&this.applyMaterialColor(r,16711680,.55)}this.rebuildNetLines(t),this.rebuildCurveVisualization(t)}rebuildNetLines(e){for(;this.netGroup.children.length;){let a=this.netGroup.children[0];a.geometry.dispose(),a.material.dispose(),this.netGroup.remove(a)}let t=e.controlPoints.length;if(!t)return;let i=e.controlPoints[0].length,r=[];for(let a=0;a<t;a++)for(let c=0;c<i-1;c++){let l=e.controlPoints[a][c],u=e.controlPoints[a][c+1];r.push(l.x,l.y,l.z,u.x,u.y,u.z)}for(let a=0;a<i;a++)for(let c=0;c<t-1;c++){let l=e.controlPoints[c][a],u=e.controlPoints[c+1][a];r.push(l.x,l.y,l.z,u.x,u.y,u.z)}let s=new Wt;s.setAttribute("position",new ut(r,3));let o=new ds({color:8965375,transparent:!0,opacity:.55});this.netGroup.add(new Ra(s,o))}rebuildCurveVisualization(e){for(this.stopCurveAnimation(),this.curveLines.clear();this.curveGroup.children.length;){let s=this.curveGroup.children[0];s.geometry.dispose(),s.material.dispose(),this.curveGroup.remove(s)}if(!e.controlPoints.length)return;let i=e.controlPoints.map(s=>s.map(o=>new it(o.x,o.y,o.z,o.w))),r=new Ll(e.degreeU,e.degreeV,e.knotsU,e.knotsV,i);this.addSurfaceCurve(r,"u",.5,16776960),this.addSurfaceCurve(r,"v",.5,4053503)}addSurfaceCurve(e,t,i,r){let s=[];for(let u=0;u<=120;u++){let d=u/120,f=new T;t==="u"?e.getPoint(d,i,f):e.getPoint(i,d,f),s.push(f)}let a=new Wa;a.setPositions(this.flattenPoints(s));let c=new Ms({color:r,transparent:!0,opacity:.95,linewidth:2.4,depthTest:!0,polygonOffset:!0,polygonOffsetFactor:-1,polygonOffsetUnits:-4});c.resolution.set(this.renderer.domElement.width,this.renderer.domElement.height);let l=new gp(a,c);l.renderOrder=20,l.frustumCulled=!1,this.curveGroup.add(l),this.curveLines.set(t,{line:l,points:s,surface:e,samples:120})}flattenPoints(e){return e.flatMap(t=>[t.x,t.y,t.z])}animateSelectedCurves(){this.selectedPatch&&(this.curveLines.size||this.rebuildCurveVisualization(this.selectedPatch),this.curveAnimationStartTime=performance.now(),this.curveAnimationActive=!this.curveAnimationActive)}setStandardView(e){let t={top:new T(0,1,0),bottom:new T(0,-1,0),front:new T(0,0,1),back:new T(0,0,-1),left:new T(-1,0,0),right:new T(1,0,0)};this.setCameraDirection(t[e].x,t[e].y,t[e].z)}setCameraDirection(e,t,i){if(!this.camera||!this.controls)return;console.log(`Setting camera direction to (${e}, ${t}, ${i})`);let r=new T(e,t,i).normalize();Math.abs(r.y)>.999&&(console.log("Nudging camera direction off the pole to avoid gimbal lock."),r.set(0,r.y,1e-4).normalize());let s=Math.max(this.camera.position.distanceTo(this.controls.target),.01),o=this.controls.target.clone().addScaledVector(r,s),a=Math.abs(r.y)>.999?new T(0,0,r.y>0?-1:1):Ni.upDirection;this.animateCameraTo(o,a)}orbitCamera(e){if(!this.camera||!this.controls)return;let t=Es.degToRad(45),i=this.camera.position.clone().sub(this.controls.target),r=new T,s=t;e==="left"||e==="right"?(r.copy(this.camera.up).normalize(),s=e==="left"?t:-t):(this.camera.getWorldDirection(r),r.cross(this.camera.up).normalize(),s=e==="up"?-t:t),i.applyAxisAngle(r,s);let o=this.controls.target.clone().add(i),a=this.camera.up.clone().applyAxisAngle(r,s);this.animateCameraTo(o,a)}animateCameraTo(e,t){let i=this.camera.clone();i.position.copy(e),i.up.copy(t),i.lookAt(this.controls.target),this.cameraAnimation={startPosition:this.camera.position.clone(),endPosition:e,startUp:this.camera.up.clone(),startQuaternion:this.camera.quaternion.clone(),endQuaternion:i.quaternion.clone(),endUp:t.clone(),upRotation:new pn().setFromUnitVectors(this.camera.up.clone().normalize(),t.clone().normalize()),startedAt:performance.now()},this.controls.enabled=!1}stopCurveAnimation(){this.curveAnimationActive=!1}updateCurveAnimation(){if(!this.curveAnimationActive||!this.curveLines.size)return;let e=performance.now()-this.curveAnimationStartTime,t=Math.min(1,e/this.curveAnimationDuration),i=Math.max(2,Math.round(t*120));this.curveLines.forEach(({line:r,points:s})=>{let o=s.slice(0,i+1);r.geometry.setPositions(this.flattenPoints(o))}),t>=1&&(this.curveAnimationActive=!1)}onCurveChange(e,t){this.curveAnimationActive=!1;let i=this.curveLines.get(t);if(!i)return;let r=parseFloat(e);console.log("param = ",r);let s=Math.max(0,Math.min(1,r)),{line:o,points:a,surface:c,samples:l}=i;for(let u=0;u<=l;u++){let d=u/l,f=a[u];t==="u"?c.getPoint(d,s,f):c.getPoint(s,d,f)}o.geometry.setPositions(this.flattenPoints(a))}updateSurfaceCurves(){if(!this.curveAnimationActive||!this.curveLines.size)return;let e=performance.now()-this.curveAnimationStartTime,t=Math.min(1,e/this.curveAnimationDuration),i=performance.now()*.001,s=(Math.sin(i*.5*Math.PI)+1)/2,o=Math.max(0,Math.min(1,s));["u","v"].forEach(a=>{let c=this.curveLines.get(a);if(!c)return;let{line:l,points:u,surface:d,samples:f}=c;for(let h=0;h<=f;h++){let g=h/f,y=u[h];a==="u"?d.getPoint(g,o,y):d.getPoint(o,g,y)}l.geometry.setPositions(this.flattenPoints(u))})}computeCpRadius(e){if(!this.patchMeshes.size)return 1;let t=this.patchMeshes.get(e.id);if(!t)return 0;let r=new Dn().setFromObject(t).getSize(new T),s=Math.max(r.x,r.y,r.z,1),o=s*.01;return Math.min(Math.max(o,.01),s*.02)}disposeControlPoints(){for(let e of this.cpSpheres.values())e.material.dispose(),this.cpGroup.remove(e);for(this.cpSpheres.clear(),this.sharedSphereGeometry?.dispose(),this.sharedSphereGeometry=null;this.netGroup.children.length;){let e=this.netGroup.children[0];e.geometry.dispose(),e.material.dispose(),this.netGroup.remove(e)}for(;this.curveGroup.children.length;){let e=this.curveGroup.children[0];e.geometry.dispose(),e.material.dispose(),this.curveGroup.remove(e)}this.curveLines.clear(),this.hoveredCpKey=null}applyMaterialColor(e,t,i){let r=e.material;r.color.setHex(t),r.emissiveIntensity=i}clearCpHover(){if(!this.hoveredCpKey)return;let e=this.cpSpheres.get(this.hoveredCpKey);e&&this.applyMaterialColor(e,16766720,.3),this.hoveredCpKey=null}rebuildMeshes(){console.log(`Rebuilding meshes for ${this.patches.length} patches...`),this.disposeMeshes();for(let e of this.patches){let t=this.createPatchMesh(e);this.patchMeshes.set(e.id,t),this.scene.add(t)}this.needsReframe&&(this.frameCameraToPatches(),this.patches.length>0&&(this.needsReframe=!1)),this.updateHighlights(),this.rebuildControlPointVisualization(),console.log(`Meshes rebuilt. Total patches: ${this.patches.length}`)}createPatchMesh(e){let t=new Oa({color:6000111,metalness:.15,roughness:.35,clearcoat:.6,clearcoatRoughness:.2,side:fi,transparent:!0,opacity:.92}),i=new tn(this.buildGeometry(e),t);return i.userData[qM]=e.id,i.castShadow=!0,i.receiveShadow=!0,i}frameCameraToPatches(){if(!this.patchMeshes.size){this.controls.target.copy(Ni.initialTarget),this.camera.position.copy(Ni.initialPosition),this.camera.up.set(0,1,0),this.camera.lookAt(Ni.initialTarget),this.controls.update();return}let e=[];for(let h of this.patches)for(let g of h.controlPoints)for(let y of g)e.push(new T(y.x,y.y,y.z));let t=new Dn().setFromPoints(e),i=t.getCenter(new T),r=t.getBoundingSphere(new Qn),s=Math.max(r.radius,1e-4),o=Es.degToRad(this.camera.fov),a=2*Math.atan(Math.tan(o/2)*this.camera.aspect),c=s/Math.sin(o/2),l=s/Math.sin(a/2),u=Ni.framingPadding*Math.max(c,l),d=i.clone().add(Ni.framingDirection.clone().multiplyScalar(u));this.controls.target.copy(i),this.camera.position.copy(d),this.camera.up.copy(Ni.upDirection),this.camera.lookAt(i),this.camera.near=Math.max(u*Ni.nearDistanceRatio,.01),this.camera.far=u*Ni.farDistanceRatio,this.camera.updateProjectionMatrix(),this.controls.maxDistance=u*Ni.maxDistanceMultiplier;let f=this.controls.enableDamping;this.controls.enableDamping=!1,this.controls.update(),this.controls.enableDamping=f,this.controls.saveState()}updateHighlights(){for(let[e,t]of this.patchMeshes){let i=t.material,r=e===this.selectedId,s=e===this.hoveredId;r?(i.color.copy(this.tempColor.setHex(16739275)),i.emissive.copy(this.tempColor.setHex(5574963)),i.emissiveIntensity=.35,i.opacity=1):s?(i.color.copy(this.tempColor.setHex(6742271)),i.emissive.copy(this.tempColor.setHex(1127236)),i.emissiveIntensity=.25,i.opacity=.98):(i.color.copy(this.tempColor.setHex(6000111)),i.emissive.setHex(0),i.emissiveIntensity=0,i.opacity=.92)}}resize(){let e=this.canvasHost.nativeElement,t=e.clientWidth,i=e.clientHeight;this.camera.aspect=t/i;let s=document.querySelector(".sidebar")?.getBoundingClientRect().width;console.log(`Sidebar width: ${s}px`);let o=16,c=document.querySelector(".app-header")?.getBoundingClientRect().height;console.log(`Header height: ${c}px`),this.camera.setViewOffset(t,i,s?(s+o)/2:o,c?c/2:0,t,i),this.camera.updateProjectionMatrix(),this.renderer.setSize(t,i,!1),this.controls.target.set(0,0,0),this.controls.update(),this.curveLines.forEach(l=>{l.line.material.resolution.set(this.renderer.domElement.width,this.renderer.domElement.height)})}animate(){this.animationFrameId=requestAnimationFrame(()=>this.animate()),this.updateSurfaceCurves(),this.updateCameraAnimation(),this.cameraAnimation||this.controls.update(),this.updateAxisTriadScale(),this.updateViewCube(),this.renderer.render(this.scene,this.camera)}updateCameraAnimation(){if(!this.cameraAnimation)return;let e=performance.now()-this.cameraAnimation.startedAt,t=Math.min(1,e/this.cameraAnimationDuration),i=t*t*(3-2*t);this.camera.position.lerpVectors(this.cameraAnimation.startPosition,this.cameraAnimation.endPosition,i),this.camera.quaternion.slerpQuaternions(this.cameraAnimation.startQuaternion,this.cameraAnimation.endQuaternion,i);let r=new pn().slerpQuaternions(new pn,this.cameraAnimation.upRotation,i);this.camera.up.copy(this.cameraAnimation.startUp).applyQuaternion(r),this.camera.lookAt(this.controls.target),t===1&&(this.camera.position.copy(this.cameraAnimation.endPosition),this.camera.up.copy(this.cameraAnimation.endUp),this.camera.lookAt(this.controls.target),this.cameraAnimation=null,this.controls.enabled=!0)}updateViewCube(){if(!this.camera)return;let e=this.camera.quaternion.clone().invert(),i=1-.26,r=31,s=(u,d,f,h)=>{let g=[0,0,0];return g[u]=d,g[(u+1)%3]=f,g[(u+2)%3]=h,g},o=(u,d,f,h,g)=>({id:u,label:d,color:f,direction:new T().setComponent(h,g),kind:"face",vertices:[s(h,g,-i,-i),s(h,g,i,-i),s(h,g,i,i),s(h,g,-i,i)]}),a=(u,d,f,h,g)=>{let y=[0,1,2].find(m=>m!==u&&m!==f),p=(m,b,M)=>{let S=[0,0,0];return S[u]=m,S[f]=b,S[y]=M,S};return{id:`edge-${u}${d}-${f}${h}`,label:g,color:"#b9c7d2",direction:new T().setComponent(u,d).add(new T().setComponent(f,h)).normalize(),kind:"chamfer",vertices:[p(d,h*i,-i),p(d*i,h,-i),p(d*i,h,i),p(d,h*i,i)]}},c=(u,d,f)=>({id:`corner-${u}${d}${f}`,label:"CORNER",color:"#d7e2e9",direction:new T(u,d,f).normalize(),kind:"corner",vertices:[[u,d*i,f*i],[u*i,d,f*i],[u*i,d*i,f]]}),l=[o("right","RIGHT","#dc5c5c",0,1),o("left","LEFT","#8e3232",0,-1),o("top","TOP","#5cbb7f",1,1),o("bottom","BOTTOM","#28663c",1,-1),o("front","FRONT","#5798df",2,1),o("back","BACK","#315f9b",2,-1),a(0,1,1,1,"TOP-RIGHT"),a(0,1,1,-1,"BOTTOM-RIGHT"),a(0,-1,1,1,"TOP-LEFT"),a(0,-1,1,-1,"BOTTOM-LEFT"),a(0,1,2,1,"FRONT-RIGHT"),a(0,1,2,-1,"BACK-RIGHT"),a(0,-1,2,1,"FRONT-LEFT"),a(0,-1,2,-1,"BACK-LEFT"),a(1,1,2,1,"TOP-FRONT"),a(1,1,2,-1,"TOP-BACK"),a(1,-1,2,1,"BOTTOM-FRONT"),a(1,-1,2,-1,"BOTTOM-BACK"),c(1,1,1),c(1,1,-1),c(1,-1,1),c(1,-1,-1),c(-1,1,1),c(-1,1,-1),c(-1,-1,1),c(-1,-1,-1)];this.viewCubeFaces=l.map(u=>{let d=u.direction.clone().applyQuaternion(e),f=u.vertices.map(([p,m,b])=>new T(p,m,b).applyQuaternion(e)),h=f.reduce((p,m)=>p+m.z,0)/f.length,g=f.map((p,m)=>`${m===0?"M":"L"} ${60+p.x*r} ${60-p.y*r}`).join(" ")+" Z",y=f.reduce((p,m)=>p.add(m),new T).multiplyScalar(1/f.length);return{id:u.id,label:u.label,color:u.color,direction:u.direction,path:g,depth:h,kind:u.kind,labelX:60+y.x*r,labelY:60-y.y*r,visible:d.z>.01}}).filter(u=>u.visible).sort((u,d)=>u.depth-d.depth).map(f=>{var h=f,{visible:u}=h,d=Ep(h,["visible"]);return d}),this.changeDetectorRef.detectChanges()}updateAxisTriadScale(){this.camera.updateMatrixWorld();let e=this.axisTriad.getWorldPosition(new T).applyMatrix4(this.camera.matrixWorldInverse),i=Math.max(-e.z,this.camera.near*2)/this.axisTriadReferenceDepth;this.axisTriad.scale.setScalar(i/2)}disposeMeshes(){for(let e of this.patchMeshes.values())e.geometry.dispose(),e.material.dispose(),this.scene?.remove(e);this.patchMeshes.clear()}createAxisTriad(){let r=[{label:"X",direction:new T(1,0,0),color:15680580},{label:"Y",direction:new T(0,1,0),color:2278750},{label:"Z",direction:new T(0,0,1),color:3900150}];for(let o of r){let a=new bl(o.direction,new T(0,0,0),10,o.color,2,.75);this.axisTriad.add(a);let c=this.createAxisLabel(o.label,o.color);c.position.copy(o.direction).multiplyScalar(10+1.5),this.axisTriad.add(c)}let s=new tn(new Pa(.6,16,12),new us({color:16777215}));s.name="origin",this.axisTriad.add(s)}createAxisLabel(e,t){let i=document.createElement("canvas");i.width=96,i.height=96;let r=i.getContext("2d");r.clearRect(0,0,i.width,i.height),r.font="bold 36px Arial, sans-serif",r.textAlign="center",r.textBaseline="middle",r.fillStyle=`#${t.toString(16).padStart(6,"0")}`,r.strokeStyle="#07101f",r.lineWidth=10,r.strokeText(e,48,51),r.fillText(e,48,51);let s=new ll(i);s.colorSpace=Cn;let o=new ol(new Aa({map:s,transparent:!0,depthTest:!1,depthWrite:!1}));return o.scale.set(4.5,4.5,1),o.name=`${e}-axis-label`,o}clonePatch(e){return ke(Ae({},e),{knotsU:[...e.knotsU],knotsV:[...e.knotsV],controlPoints:e.controlPoints.map(t=>t.map(i=>Ae({},i)))})}formatKnots(e){let i=o=>o.toPrecision(5).replace(/\.?0+$/,"");if(e.length<=6)return e.map(i).join(" ");let r=e.slice(0,3).map(i).join(" "),s=e.slice(-2).map(i).join(" ");return`${r} \u2026 ${s} (${e.length})`}savePatchesToJson(){if(!this.patches.length)return;let e={version:"1.0",exportedAt:new Date().toISOString(),patchCount:this.patches.length,patches:this.patches},t=new Blob([JSON.stringify(e,null,2)],{type:"application/json;charset=utf-8"}),i=URL.createObjectURL(t),r=document.createElement("a");r.href=i,r.download="nurbs-model.json",document.body.appendChild(r),r.click(),r.remove(),setTimeout(()=>URL.revokeObjectURL(i),0)}loadPatchesFromJson(e){let t=e.target;if(!t.files?.length)return;let i=t.files[0],r=new FileReader;r.onload=s=>{try{let o=s.target?.result,a=JSON.parse(o);a&&a.patches&&Array.isArray(a.patches)?this.patchesLoaded.emit(a.patches):console.error("Invalid JSON format: missing patches array")}catch(o){console.error("Failed to parse JSON file",o)}finally{t.value=""}},r.readAsText(i)}static \u0275fac=function(t){return new(t||n)(ct(ts),ct(Rt),ct(Yi))};static \u0275cmp=yr({type:n,selectors:[["app-viewport"]],viewQuery:function(t,i){if(t&1&&Pd(C2,7),t&2){let r;rv(r=sv())&&(i.canvasHost=r.first)}},inputs:{patches:"patches",selectedId:"selectedId",hoveredId:"hoveredId",selectedPatch:"selectedPatch"},outputs:{patchHover:"patchHover",patchSelect:"patchSelect",patchDragComplete:"patchDragComplete",patchesLoaded:"patchesLoaded",exportStep:"exportStep"},features:[gr],decls:28,vars:5,consts:[["canvasHost",""],[1,"viewport-host"],[1,"viewport-actions"],[1,"slider-group"],["for","u-curve-slider",1,"slider-label"],["type","range","id","u-curve-slider","min","0","max","1","step","0.01",3,"input","disabled"],["for","v-curve-slider",1,"slider-label"],["type","range","id","v-curve-slider","min","0","max","1","step","0.01",3,"input","disabled"],["type","button","aria-label","Animate selected patch U and V curves",1,"animate-curves-btn",3,"click","disabled"],["aria-label","Standard camera views",1,"view-triad"],["type","button","aria-label","Rotate view up 45 degrees","title","Rotate up 45 degrees",1,"view-control","view-control--top",3,"click"],["type","button","aria-label","Rotate view left 45 degrees","title","Rotate left 45 degrees",1,"view-control","view-control--left",3,"click"],["viewBox","0 0 120 120","aria-label","View cube",1,"view-cube"],["type","button","aria-label","Rotate view right 45 degrees","title","Rotate right 45 degrees",1,"view-control","view-control--right",3,"click"],["type","button","aria-label","Rotate view down 45 degrees","title","Rotate down 45 degrees",1,"view-control","view-control--bottom",3,"click"],[1,"cube-face",3,"click"],[1,"cube-label"]],template:function(t,i){t&1&&(En(0,"div",1,0)(2,"div",2)(3,"div",3)(4,"div")(5,"label",4),Ee(6,"U Curve"),Sn(),En(7,"input",5),xi("input",function(s){return i.onCurveChange(s.target.value,"u")}),Sn()(),En(8,"div")(9,"label",6),Ee(10,"V Curve"),Sn(),En(11,"input",7),xi("input",function(s){return i.onCurveChange(s.target.value,"v")}),Sn()()(),En(12,"button",8),xi("click",function(){return i.animateSelectedCurves()}),Ee(13," Animate Curves "),Sn()(),En(14,"nav",9)(15,"button",10),xi("click",function(){return i.orbitCamera("up")}),Ee(16,"\u25B2"),Sn(),En(17,"button",11),xi("click",function(){return i.orbitCamera("left")}),Ee(18,"\u25C0"),Sn(),Oo(),En(19,"svg",12)(20,"title"),Ee(21,"Camera orientation cube. Click a visible face to view it directly."),Sn(),_r(22,A2,4,6,null,null,T2),Sn(),Gu(),En(24,"button",13),xi("click",function(){return i.orbitCamera("right")}),Ee(25,"\u25B6"),Sn(),En(26,"button",14),xi("click",function(){return i.orbitCamera("down")}),Ee(27,"\u25BC"),Sn()()()),t&2&&(Ce(3),dn("disabled-group",!i.selectedPatch),Ce(4),Xo("disabled",!i.selectedPatch),Ce(4),Xo("disabled",!i.selectedPatch),Ce(),Xo("disabled",!i.selectedPatch),Ce(10),xr(i.viewCubeFaces))},styles:["[_nghost-%COMP%]{display:block;width:100%;height:100%;min-height:0}.viewport-host[_ngcontent-%COMP%]{position:relative;width:100%;height:100%;border-radius:inherit;overflow:hidden}.viewport-host[_ngcontent-%COMP%]   canvas[_ngcontent-%COMP%]{display:block;width:100%;height:100%}.viewport-actions[_ngcontent-%COMP%]{position:absolute;top:1rem;left:1rem;z-index:20;display:flex;gap:.6rem;flex-wrap:wrap;align-items:center}.animate-curves-btn[_ngcontent-%COMP%]{align-self:center;border:1px solid rgba(255,255,255,.16);padding:.55rem .9rem;border-radius:8px;white-space:nowrap;background:#0a1226e6;color:#f2f7ff;font-size:.8rem;font-weight:600;cursor:pointer;box-shadow:0 8px 24px #00000047;-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);transition:transform .15s ease,background .15s ease,opacity .15s ease}.animate-curves-btn[_ngcontent-%COMP%]:hover:not(:disabled){transform:translateY(-1px);background:#111f3df2}.animate-curves-btn[_ngcontent-%COMP%]:disabled{opacity:.6;cursor:not-allowed}.view-triad[_ngcontent-%COMP%]{position:absolute;top:.8rem;right:.8rem;z-index:30;display:grid;grid-template-columns:2.4rem 8rem 2.4rem;grid-template-rows:2.2rem 8rem 2.2rem;align-items:center;justify-items:center;width:14rem;padding:.35rem;border:1px solid rgba(181,212,244,.24);border-radius:.8rem;background:#08101f1f;box-shadow:0 8px 24px #00000057}.view-control[_ngcontent-%COMP%]{border:0;color:#c9d8e8;cursor:pointer;font:700 .58rem/1 Arial,sans-serif;transition:color .15s ease,transform .15s ease,filter .15s ease}.view-control[_ngcontent-%COMP%]{padding:.2rem;background:transparent}.view-control[_ngcontent-%COMP%]:hover, .view-control[_ngcontent-%COMP%]:focus-visible{color:#fff;filter:drop-shadow(0 0 5px rgba(116,191,255,.8));outline:none}.view-control[_ngcontent-%COMP%]:active{transform:scale(.92)}.view-control--top[_ngcontent-%COMP%]{grid-column:2;grid-row:1;font-size:1.2rem}.view-control--left[_ngcontent-%COMP%]{grid-column:1;grid-row:2;font-size:1.2rem}.view-control--right[_ngcontent-%COMP%]{grid-column:3;grid-row:2;font-size:1.2rem}.view-control--bottom[_ngcontent-%COMP%]{grid-column:2;grid-row:3;font-size:1.2rem}.view-cube[_ngcontent-%COMP%]{grid-column:2;grid-row:2;width:8rem;height:8rem;overflow:visible}.cube-face[_ngcontent-%COMP%]{cursor:pointer;stroke:#cce5f7b8;stroke-width:1.2;transition:filter .15s ease,fill .15s ease}.cube-face[_ngcontent-%COMP%]:hover{fill:#d2ecff;filter:drop-shadow(0 0 4px rgba(116,191,255,.95))}.cube-chamfer[_ngcontent-%COMP%]{stroke:#edf7ff;stroke-width:1.4}.cube-label[_ngcontent-%COMP%]{pointer-events:none;fill:#f7fbff;font:700 9px Arial,sans-serif;letter-spacing:.03em;text-anchor:middle;dominant-baseline:middle;paint-order:stroke;stroke:#0000005c;stroke-width:1.3}.patch-hud[_ngcontent-%COMP%]{position:absolute;top:11rem;right:1rem;min-width:200px;max-width:280px;padding:.75rem .9rem;border-radius:.85rem;background:#080c1cd1;-webkit-backdrop-filter:blur(14px);backdrop-filter:blur(14px);border:1px solid rgba(255,255,255,.12);box-shadow:0 8px 32px #00000073,inset 0 1px #ffffff0f;display:flex;flex-direction:column;gap:.4rem;pointer-events:none;font-size:.75rem;color:#d4e6ff;animation:_ngcontent-%COMP%_hud-fade-in .22s ease;z-index:10}@keyframes _ngcontent-%COMP%_hud-fade-in{0%{opacity:0;transform:translateY(-6px) scale(.97)}to{opacity:1;transform:translateY(0) scale(1)}}.hud-title[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.4rem;font-size:.8rem;font-weight:700;color:#fff;border-bottom:1px solid rgba(255,255,255,.1);padding-bottom:.4rem;margin-bottom:.1rem}.hud-icon[_ngcontent-%COMP%]{color:gold;filter:drop-shadow(0 0 4px rgba(255,215,0,.5))}.hud-row[_ngcontent-%COMP%]{display:flex;align-items:baseline;justify-content:space-between;gap:.5rem}.hud-row--knot[_ngcontent-%COMP%]{align-items:flex-start}.hud-label[_ngcontent-%COMP%]{color:#ffffff7a;white-space:nowrap;flex-shrink:0}.hud-value[_ngcontent-%COMP%]{font-weight:600;color:#9ed0ff;text-align:right}.hud-knot[_ngcontent-%COMP%]{font-family:Fira Code,Cascadia Code,monospace;font-size:.66rem;color:#b8d8ff;word-break:break-all;text-align:right}.hud-hint[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.35rem;margin-top:.25rem;padding-top:.35rem;border-top:1px solid rgba(255,255,255,.07);font-size:.66rem;color:#ffffff61}.hud-hint-dot[_ngcontent-%COMP%]{color:gold;font-size:.5rem}.slider-group[_ngcontent-%COMP%]{display:flex;flex-direction:row;gap:.8rem;padding:1rem;border:1px solid rgba(255,255,255,.1);background:#0a122699;-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border-radius:.8rem}.slider-label[_ngcontent-%COMP%]{font-size:.75rem;font-weight:600;color:#c9d8e8;margin-bottom:-.2rem}.slider-group[_ngcontent-%COMP%] > div[_ngcontent-%COMP%]{flex:1}.slider-group[_ngcontent-%COMP%] > div[_ngcontent-%COMP%]:not(:first-child){border-left:1px solid #1e293b;padding-left:24px}.slider-group.disabled-group[_ngcontent-%COMP%]{opacity:.4;pointer-events:none;cursor:not-allowed}.slider-group.disabled-group[_ngcontent-%COMP%]   input[type=range][_ngcontent-%COMP%]{background:#334155}input[type=range][_ngcontent-%COMP%]{appearance:none;-webkit-appearance:none;width:100%;height:4px;background:#ffffff1a;border-radius:2px;outline:none}input[type=range][_ngcontent-%COMP%]::-webkit-slider-thumb{appearance:none;width:14px;height:14px;background:#9ed0ff;border:2px solid rgba(255,255,255,.8);border-radius:50%;cursor:pointer;transition:transform .1s ease,background .1s ease}input[type=range][_ngcontent-%COMP%]:active::-webkit-slider-thumb{transform:scale(1.2);background:#fff}input[type=range][_ngcontent-%COMP%]::-moz-range-thumb{width:14px;height:14px;background:#9ed0ff;border:2px solid rgba(255,255,255,.8);border-radius:50%;cursor:pointer}"]})};var I2=6,yp=class n{oc=null;initPromise=null;initialize(){return cn(this,null,function*(){return this.oc?this.oc:(this.initPromise||(this.initPromise=this.loadOpenCascade()),this.oc=yield this.initPromise,this.oc)})}parseStepFile(e){return cn(this,null,function*(){let t=yield this.initialize(),i="/test.step",r=new Uint8Array(yield e.arrayBuffer());try{t.FS.writeFile(i,r)}catch(s){console.error("writeFile failed",s)}for(let s of Object.keys(t))s.includes("STEPControl_Reader");try{let s=new t.STEPControl_Reader_1,o=s.ReadFile(i),a=s.TransferRoots(),c=s.OneShape(),l=this.extractNurbsPatches(t,c);return this.normalizePatchesToOrigin(l)}finally{try{t.FS.unlink(i)}catch(s){}}})}exportStepFile(e){return cn(this,null,function*(){let t=1,i=()=>t++,r=t++,s=t++,o=t++,a=t++,c=t++,l=t++,u=t++,d=t++,f=t++,h=t++,g=t++,y=t++,p=t++,m=t++,b=t++,M=[],S=[];for(let I of e){let _=I.degreeU,C=I.degreeV,O=I.controlPoints.length,D=I.controlPoints[0]?.length||0;if(O===0||D===0)continue;let{knots:L,multiplicities:G}=this.compressKnots(I.knotsU),{knots:W,multiplicities:P}=this.compressKnots(I.knotsV),H=[];for(let Ke=0;Ke<O;Ke++){let Fe=[];for(let X=0;X<D;X++){let le=I.controlPoints[Ke][X],ee=t++;S.push(`#${ee}=CARTESIAN_POINT('',(${le.x.toFixed(8)},${le.y.toFixed(8)},${le.z.toFixed(8)}));`),Fe.push(ee)}H.push(Fe)}let U=`(${H.map(Ke=>`(${Ke.map(Fe=>`#${Fe}`).join(",")})`).join(",")})`,J=`(${I.controlPoints.map(Ke=>`(${Ke.map(Fe=>Fe.w.toFixed(8)).join(",")})`).join(",")})`,Q=`(${L.map(Ke=>Ke.toFixed(8)).join(",")})`,ce=`(${W.map(Ke=>Ke.toFixed(8)).join(",")})`,xe=`(${G.join(",")})`,Se=`(${P.join(",")})`,rt=t++;M.push(rt),S.push(`#${rt}=(
BOUNDED_SURFACE()
B_SPLINE_SURFACE(${_},${C},${U},.UNSPECIFIED.,.F.,.F.,.F.)
B_SPLINE_SURFACE_WITH_KNOTS(${xe},${Se},${Q},${ce},.UNSPECIFIED.)
GEOMETRIC_REPRESENTATION_ITEM()
RATIONAL_B_SPLINE_SURFACE(${J})
REPRESENTATION_ITEM('')
SURFACE()
);`)}let A=M.map(I=>`#${I}`).join(","),w=["ISO-10303-21;","HEADER;","FILE_DESCRIPTION(('NURBS Surface Export'),'2;1');","FILE_NAME('export.step','',(''),(''),'NURBS Surface Viewer','','');","FILE_SCHEMA(('AUTOMOTIVE_DESIGN'));","ENDSEC;","DATA;",`#${r}=APPLICATION_PROTOCOL_DEFINITION('international standard','automotive_design',2000,#${s});`,`#${s}=APPLICATION_CONTEXT('core data for automotive mechanical design processes');`,`#${o}=PRODUCT('NURBS Model','NURBS Model','',(#${a}));`,`#${a}=PRODUCT_CONTEXT('',#${s},'mechanical');`,`#${c}=PRODUCT_DEFINITION_FORMATION('','',#${o});`,`#${l}=PRODUCT_DEFINITION_CONTEXT('part definition',#${s},'design');`,`#${u}=PRODUCT_DEFINITION('design','',#${c},#${l});`,`#${d}=PRODUCT_DEFINITION_SHAPE('','',#${u});`,`#${f}=SHAPE_DEFINITION_REPRESENTATION(#${d},#${h});`,`#${h}=GEOMETRICALLY_BOUNDED_SURFACE_SHAPE_REPRESENTATION('',(${A}),#${g});`,`#${g}=(`,"GEOMETRIC_REPRESENTATION_CONTEXT(3)",`GLOBAL_UNCERTAINTY_ASSIGNED_CONTEXT((#${y}))`,`GLOBAL_UNIT_ASSIGNED_CONTEXT((#${p},#${m},#${b}))`,"REPRESENTATION_CONTEXT('Context #1','3D Context with UNIT and UNCERTAINTY')",");",`#${y}=UNCERTAINTY_MEASURE_WITH_UNIT(LENGTH_MEASURE(1.E-7),#${p},'distance_accuracy_value','Confusion Distance');`,`#${p}=(LENGTH_UNIT() NAMED_UNIT(*) SI_UNIT(.MILLI.,.METRE.));`,`#${m}=(ANGLE_UNIT() NAMED_UNIT(*) SI_UNIT($,.RADIAN.));`,`#${b}=(NAMED_UNIT(*) SI_UNIT($,.STERADIAN.) SOLID_ANGLE_UNIT());`,...S,"ENDSEC;","END-ISO-10303-21;"];return new Blob([w.join(`
`)],{type:"application/step"})})}compressKnots(e){let t=[],i=[];if(e.length===0)return{knots:t,multiplicities:i};let r=e[0],s=1;t.push(r);for(let o=1;o<e.length;o++)Math.abs(e[o]-r)<1e-6?s++:(i.push(s),r=e[o],t.push(r),s=1);return i.push(s),{knots:t,multiplicities:i}}loadOpenCascade(){return window.__openCascadePromise||(window.__openCascadePromise=new Promise((e,t)=>{let i=s=>{window.removeEventListener("occt-ready",i),e(s.detail)};window.addEventListener("occt-ready",i);let r=document.createElement("script");r.type="module",r.textContent=`
        import initOpenCascade from '/opencascade.wasm.js';
        const oc = await initOpenCascade({
          locateFile(path) {
            return path.endsWith('.wasm') ? '/opencascade.wasm.wasm' : path;
          }
        });
        window.dispatchEvent(new CustomEvent('occt-ready', { detail: oc }));
      `,r.onerror=()=>{window.removeEventListener("occt-ready",i),t(new Error("Failed to load OpenCascade.js runtime."))},document.head.appendChild(r)})),window.__openCascadePromise}extractNurbsPatches(e,t){let i=[],r=new e.TopExp_Explorer_2(t,e.TopAbs_ShapeEnum.TopAbs_FACE,e.TopAbs_ShapeEnum.TopAbs_SHAPE);console.log("More =",r.More()),console.log(t),console.log("shape null =",t.IsNull());let s=0;for(;r.More();r.Next()){s+=1;let o=e.TopoDS.Face_1(r.Current()),a=this.extractPatchFromFace(e,o,s);a&&i.push(a)}return console.log(`Extracted ${i.length} NURBS patches from the STEP file.`),i}extractPatchFromFace(e,t,i){let r=e.BRep_Tool.Surface_2(t),s=new e.GeomAdaptor_Surface_2(r),o=s.GetType();if(console.log("Surface type =",o),o.value!=6)return console.log("Surface is already a B-Spline surface."),null;let c=s.BSpline().get();console.log("bspline:",c),console.log("V Degree:",c.VDegree());let l=c.UDegree(),u=c.VDegree(),d=c.NbUPoles(),f=c.NbVPoles(),h=c.IsURational()||c.IsVRational(),g=[];for(let m=1;m<=d;m++){let b=[];for(let M=1;M<=f;M++){let S=c.Pole(m,M),A=h?c.Weight(m,M):1;b.push({x:S.X(),y:S.Y(),z:S.Z(),w:A})}g.push(b)}let y=this.expandKnots(this.copyRealArray(c.UKnots_2()),this.copyIntArray(c.UMultiplicities_2())),p=this.expandKnots(this.copyRealArray(c.VKnots_2()),this.copyIntArray(c.VMultiplicities_2()));return{id:`patch-${i}`,name:`NURBS Face ${i}`,degreeU:l,degreeV:u,knotsU:y,knotsV:p,controlPoints:g}}getBSplineFromFace(e,t){try{let i=new e.BRepAdaptor_Surface(t,!0);if(i.GetType()===I2){let r=i.BSpline();if(!r.IsNull())return r}}catch(i){}try{let i=e.BRep_Tool.Surface_1(t),r=e.Geom_BSplineSurface.DownCast(i);return r.IsNull()?null:r}catch(i){return null}}copyRealArray(e){let t=[];for(let i=e.Lower();i<=e.Upper();i++)t.push(e.Value(i));return t}normalizePatchesToOrigin(e){if(e.length===0)return e;let t=Number.POSITIVE_INFINITY,i=Number.POSITIVE_INFINITY,r=Number.POSITIVE_INFINITY,s=Number.NEGATIVE_INFINITY,o=Number.NEGATIVE_INFINITY,a=Number.NEGATIVE_INFINITY;for(let d of e)for(let f of d.controlPoints)for(let h of f)t=Math.min(t,h.x),i=Math.min(i,h.y),r=Math.min(r,h.z),s=Math.max(s,h.x),o=Math.max(o,h.y),a=Math.max(a,h.z);let c=(t+s)/2,l=(i+o)/2,u=(r+a)/2;return e.map(d=>ke(Ae({},d),{controlPoints:d.controlPoints.map(f=>f.map(h=>ke(Ae({},h),{x:h.x-c,y:h.y-l,z:h.z-u})))}))}copyIntArray(e){let t=[];for(let i=e.Lower();i<=e.Upper();i++)t.push(e.Value(i));return t}expandKnots(e,t){let i=[];for(let r=0;r<e.length;r++){let s=t[r]??1;for(let o=0;o<s;o++)i.push(e[r])}return i}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ut({token:n,factory:n.\u0275fac,providedIn:"root"})};var _p=class n{constructor(e,t,i){this.cadLoader=e;this.selectionService=t;this.cdr=i}cadLoader;selectionService;cdr;patches=[];selectedId=null;hoveredId=null;loading=!1;error=null;fileName=null;subscriptions=new Ft;ngOnInit(){this.subscriptions.add(this.selectionService.selectedId$.subscribe(e=>{this.selectedId=e})),this.subscriptions.add(this.selectionService.hoveredId$.subscribe(e=>{this.hoveredId=e}))}ngOnDestroy(){this.subscriptions.unsubscribe()}get selectedPatch(){return this.patches.find(e=>e.id===this.selectedId)??null}onFileSelected(e){return cn(this,null,function*(){this.loading=!0,this.error=null,this.fileName=e.name,this.selectionService.clear();try{let t=yield this.cadLoader.parseStepFile(e);console.log("Inside Angular zone?",Rt.isInAngularZone()),t.length===0&&(this.error="No NURBS/B-Spline surfaces were found in this STEP file."),this.patches=t,console.log("patches set at",performance.now())}catch(t){this.patches=[],this.error=t instanceof Error?t.message:"Failed to parse STEP file."}finally{this.loading=!1,this.cdr.detectChanges()}})}onPatchHover(e){this.selectionService.hover(e)}onPatchSelect(e){this.selectionService.select(e)}onTreePatchSelect(e){this.selectionService.select(e)}onPatchEdited(e){this.patches=this.patches.map(t=>t.id===e.id?e:t)}onPatchDragComplete(e){this.patches=this.patches.map(t=>t.id===e.id?e:t)}onPatchesLoaded(e){this.patches=e,this.fileName="Loaded from JSON",this.selectionService.clear(),this.cdr.detectChanges()}onExportStep(){return cn(this,null,function*(){if(this.patches.length){this.loading=!0;try{let e=yield this.cadLoader.exportStepFile(this.patches),t=URL.createObjectURL(e),i=document.createElement("a");i.href=t,i.download="exported-model.step",document.body.appendChild(i),i.click(),i.remove(),setTimeout(()=>URL.revokeObjectURL(t),0)}catch(e){console.error("Failed to export STEP file:",e),this.error=e instanceof Error?e.message:"Failed to export STEP file."}finally{this.loading=!1,this.cdr.detectChanges()}}})}static \u0275fac=function(t){return new(t||n)(ct(yp),ct(ts),ct(Yi))};static \u0275cmp=yr({type:n,selectors:[["app-root"]],decls:14,vars:11,consts:[[1,"app-shell"],[1,"app-header"],[1,"eyebrow"],[1,"subtitle"],[1,"workspace"],[1,"sidebar","glass-panel"],[3,"fileSelected","patchHover","patchSelect","patchEdited","patchesLoaded","exportStep","patches","selectedId","hoveredId","loading","error","fileName","selectedPatch"],[1,"viewport-panel","glass-panel"],[3,"patchHover","patchSelect","patchDragComplete","patchesLoaded","exportStep","patches","selectedId","hoveredId","selectedPatch"]],template:function(t,i){t&1&&(pe(0,"div",0)(1,"header",1)(2,"div")(3,"p",2),Ee(4,"NURBS Surface Viewer"),ge(),pe(5,"h1"),Ee(6,"STEP to Interactive Patches"),ge()(),pe(7,"p",3),Ee(8," Import a STEP file, explore extracted B-spline surfaces in 3D, and sync selection with the feature tree. "),ge()(),pe(9,"main",4)(10,"aside",5)(11,"app-feature-tree",6),Pt("fileSelected",function(s){return i.onFileSelected(s)})("patchHover",function(s){return i.onPatchHover(s)})("patchSelect",function(s){return i.onTreePatchSelect(s)})("patchEdited",function(s){return i.onPatchEdited(s)})("patchesLoaded",function(s){return i.onPatchesLoaded(s)})("exportStep",function(){return i.onExportStep()}),ge()(),pe(12,"section",7)(13,"app-viewport",8),Pt("patchHover",function(s){return i.onPatchHover(s)})("patchSelect",function(s){return i.onPatchSelect(s)})("patchDragComplete",function(s){return i.onPatchDragComplete(s)})("patchesLoaded",function(s){return i.onPatchesLoaded(s)})("exportStep",function(){return i.onExportStep()}),ge()()()()),t&2&&(Ce(11),bn("patches",i.patches)("selectedId",i.selectedId)("hoveredId",i.hoveredId)("loading",i.loading)("error",i.error)("fileName",i.fileName)("selectedPatch",i.selectedPatch),Ce(2),bn("patches",i.patches)("selectedId",i.selectedId)("hoveredId",i.hoveredId)("selectedPatch",i.selectedPatch))},dependencies:[tf,vp],styles:["[_nghost-%COMP%]{display:block;min-height:100vh;color:#f4f7ff;background:radial-gradient(circle at top left,rgba(91,141,239,.28),transparent 32%),radial-gradient(circle at top right,rgba(255,107,203,.22),transparent 28%),linear-gradient(160deg,#090d18,#11182b 45%,#0a1020)}.app-shell[_ngcontent-%COMP%]{min-height:100vh;padding:1.5rem;box-sizing:border-box;display:flex;flex-direction:column;gap:1.25rem}.app-header[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:.5rem}.eyebrow[_ngcontent-%COMP%]{margin:0;font-size:.78rem;letter-spacing:.14em;text-transform:uppercase;color:#ffffff8c}.app-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0;font-size:clamp(1.8rem,3vw,2.6rem);font-weight:700;background:linear-gradient(90deg,#fff,#9ed0ff 55%,#ff9ce6);-webkit-background-clip:text;background-clip:text;color:transparent}.subtitle[_ngcontent-%COMP%]{margin:0;max-width:48rem;color:#ffffffad;line-height:1.5}.workspace[_ngcontent-%COMP%]{display:grid;grid-template-columns:minmax(280px,360px) minmax(0,1fr);gap:1rem;height:calc(100vh - 180px);min-height:0}.glass-panel[_ngcontent-%COMP%]{border:1px solid rgba(255,255,255,.12);border-radius:1.25rem;background:#ffffff0f;-webkit-backdrop-filter:blur(18px);backdrop-filter:blur(18px);box-shadow:0 20px 60px #00000047,inset 0 1px #ffffff14}.sidebar[_ngcontent-%COMP%]{padding:1.1rem;height:100%;min-height:0;display:flex;flex-direction:column}.viewport-panel[_ngcontent-%COMP%]{height:100%;min-height:0;overflow:hidden}@media(max-width:960px){.workspace[_ngcontent-%COMP%]{grid-template-columns:1fr}.sidebar[_ngcontent-%COMP%], .viewport-panel[_ngcontent-%COMP%]{min-height:50vh}}"]})};Ev(_p,yE).catch(n=>console.error(n));
