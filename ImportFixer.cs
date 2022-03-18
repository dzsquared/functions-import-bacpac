// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.SqlServer.Dac.Deployment;
using Microsoft.SqlServer.Dac.Model;
using System.Collections.Generic;

namespace Azure.Samples
{
    [ExportDeploymentPlanModifier(ImportFixer.ImportFixerContributorId, "1.0.0.0")]
    public class ImportFixer : DeploymentPlanModifier
    {
        public const string ImportFixerContributorId = "Azure.Samples.ImportFixer";

        protected override void OnExecute(DeploymentPlanContributorContext context) {
            DeploymentStep next = context.PlanHandle.Head;
            List<DeploymentStep> triggerSteps = new List<DeploymentStep>();

            while (next != null)
            {
                DeploymentStep current = next;
                next = current.Next;

                CreateElementStep createElementStep = current as CreateElementStep;
                // assuming this is on bacpac import, all steps will be createelementsteps
                // https://docs.microsoft.com/dotnet/api/microsoft.sqlserver.dac.deployment.createelementstep
                if (createElementStep != null && createElementStep.SourceElement != null)
                {
                    // if the element is a trigger, then move it to end
                    // https://docs.microsoft.com/dotnet/api/microsoft.sqlserver.dac.model
                    if (DmlTrigger.TypeClass.Equals(createElementStep.SourceElement.ObjectType))
                    {
                        DeploymentStep triggerStep = current;
                        base.Remove(context.PlanHandle, current);
                        triggerSteps.Add(triggerStep);
                    }
                }
            }

            // add the triggers back in on the end
            foreach (DeploymentStep triggerStep in triggerSteps)
            {
                base.AddAfter(context.PlanHandle, context.PlanHandle.Tail, triggerStep);
            }
        }
    }

}