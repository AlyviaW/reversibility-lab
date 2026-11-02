/* Reversibility Canvas  ·  explicit research-prototype decision rules.
   Heuristics are uncalibrated. They are not a predictive model or legal advice. */
(function(root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.RCEngine = api;
})(typeof window !== 'undefined' ? window : globalThis, function(){
  'use strict';
  const dimensions = ['financial','time','contract','operational','reputation','stakeholders','specificity'];
  const five = [
    ['fit','Evidence · Claim Fit',25], ['viability','Commercial Viability',25],
    ['exposure','Reversibility Exposure',20], ['optionality','Strategic Optionality',15],
    ['exit','Exit Executability',15]
  ];
  const thresholdNames = ['Undetermined','Exploratory','Direct validation','Independent corroboration'];
  function profileSummary(record) {
    const levels = dimensions.map(d => String(record?.profile?.[d] ?? 'unknown'));
    const unknown = levels.filter(x=>x === 'unknown').length;
    if (unknown) return {tier:0,name:'Awaiting assessment',reason:`${unknown} reversibility dimension(s) unassessed`,unknown};
    const high = levels.filter(x => x === 'high').length;
    const moderate = levels.filter(x => x === 'moderate').length;
    let tier = high ? 3 : moderate >= 2 ? 2 : 1;
    let reason = high ? `${high} high reversibility exposure(s)` : moderate >= 2 ? `${moderate} moderate exposures` : 'Low or limited exposures';
    // Explicit comparison to the entrepreneur's affordable loss. No invented value for missing.
    const loss = Number(record?.decision?.unrecoverable);
    const budget = Number(record?.decision?.affordable);
    if (record?.decision?.unrecoverable !== '' && record?.decision?.affordable !== '' &&
       Number.isFinite(loss) && Number.isFinite(budget) && budget > 0 && loss > budget) {
      tier = 3; reason += '; estimated unrecoverable loss exceeds stated affordable loss';
    }
    return {tier,name:['','Low','Moderate','High'][tier],reason,unknown:0};
  }
  function evidenceSummary(record, requiredTier) {
    const e = record?.evidence || {};
    const strength = Number(e.strength || 0);
    if (!requiredTier) return {adequate:false,status:'Unknown',explanation:'Assess the reversibility profile first.'};
    if (!record?.assumption?.text?.trim()) return {adequate:false,status:'Incomplete',explanation:'A specific, testable critical assumption is required.'};
    if (!e.source?.trim() || !e.observation?.trim()) return {adequate:false,status:'Incomplete',explanation:'Record the evidence observation and provenance.'};
    if (!record?.decision?.date || !e.sourceDate) return {adequate:false,status:'Provenance date missing',explanation:'Record both decision date and evidence availability date; evidence must be available when the decision is made.'};
    if (String(e.sourceDate) > String(record.decision.date)) return {adequate:false,status:'Post-decision evidence',explanation:'This evidence first became available after the decision date and belongs to subsequent evaluation of the original commitment.'};
    if (e.relevance !== 'yes') return {adequate:false,status:'Review',explanation:'Evidence relevance to this decision is unconfirmed.'};
    if (strength < requiredTier) return {adequate:false,status:'Below threshold',explanation:`Evidence level ${strength} is below required level ${requiredTier}.`};
    if (requiredTier >= 2 && e.disconfirm !== 'yes') return {adequate:false,status:'Review',explanation:'Check a credible disconfirming explanation for a material commitment.'};
    if (requiredTier === 3 && e.independent !== 'yes') return {adequate:false,status:'Below threshold',explanation:'High exposure requires independently corroborated, decision-relevant behavioural evidence.'};
    return {adequate:true,status:'Threshold provisionally met',explanation:'The prototype evidence rubric is met; the assumption remains subject to continuing observation.'};
  }
  function evaluate(record) {
    const profile = profileSummary(record);
    const evidence = evidenceSummary(record,profile.tier);
    const g = record?.gates || {};
    let action, reason;
    if (g.legal === 'blocked' || g.harm === 'yes') {
      action = 'BLOCK / REDESIGN'; reason = 'A regulatory restriction or unacceptable harm activates the prior decision gate.';
    } else if (!record?.decision?.action?.trim()) {
      action = 'DEFINE THE DECISION'; reason = 'A concrete commitment supplies the unit of assessment and the starting point for this record.';
    } else if (g.legal !== 'clear' || g.harm !== 'no') {
      action = 'VERIFY HARD CONSTRAINT'; reason = 'Legal authority or stakeholder impact requires further verification.';
    } else if (profile.tier === 0) {
      action = 'CLARIFY EXPOSURE'; reason = 'Unknown reversibility remains open for assessment and source review.';
    } else if (!evidence.adequate) {
      action = record?.decision?.lowerCostPossible === 'yes' ? 'TEST SMALLER / RENEGOTIATE' : 'GATHER EVIDENCE / DEFER';
      reason = evidence.explanation;
    } else if (record?.exit?.right !== 'yes' || !record?.exit?.signal?.trim() || !record?.exit?.threshold?.trim() || !record?.exit?.owner?.trim()) {
      action = 'DESIGN EXECUTABLE EXIT'; reason = 'Pre-specified signals and actual exit authority are needed to support continuation.';
    } else {
      action = 'PROCEED WITH MONITORING'; reason = 'Decision-specific evidence, gates and an executable exit are provisionally documented.';
    }
    return {profile,evidence,requiredTier:profile.tier,requiredLabel:thresholdNames[profile.tier],action,reason};
  }
  function composite(scores, weights) {
    if (!weights || weights.length !== 5 || weights.some(w => !Number.isFinite(+w) || +w<0) || Math.abs(weights.reduce((a,b)=>a+Number(b),0)-100)>0.00001) {
      return {status:'Invalid weights',value:null};
    }
    if (!scores || scores.length!==5 || scores.some(s=> s===null || s==='' || s===undefined || !Number.isFinite(+s) || +s<0 || +s>4)) {
      return {status:'Awaiting assessment',value:null};
    }
    return {status:'Illustrative only',value:Math.round(scores.reduce((v,s,i)=>v+Number(s)*weights[i]/400,0)*1000)/10};
  }
  function alternativeTier(option) {
    const dim = {profile:option.profile || {},decision:{unrecoverable:option.unrecoverable ?? '',affordable:option.affordable ?? ''}};
    return profileSummary(dim);
  }
  return {dimensions,five,thresholdNames,profileSummary,evidenceSummary,evaluate,composite,alternativeTier};
});
