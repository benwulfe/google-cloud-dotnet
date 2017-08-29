// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Google.Cloud.Tools.DataGenerator
{
    public class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("usage...");
                return;
            }
            HashSet<string> sigs = new HashSet<string>();
            var folder = new DirectoryInfo(args[0]);
            var files = folder.GetFiles("*.cs").ToList();
            foreach (var file in files)
            {
                using (var reader = file.OpenText())
                {
                    var text = SourceText.From(reader, (int)file.Length);
                    var root = CSharpSyntaxTree.ParseText(text).GetRoot();
                    var classNode = root
                        .DescendantNodes()
                        .OfType<ClassDeclarationSyntax>()
                        .FirstOrDefault(x => x.Identifier.ToString().EndsWith("Client", StringComparison.Ordinal));
                    if (classNode != null)
                    {
                        foreach (var asyncMethod in classNode.DescendantNodes()
                            .OfType<MethodDeclarationSyntax>()
                            .Where(method => method.Identifier.ToString().EndsWith("Async")))
                        {
                            var sig = $"{asyncMethod.ReturnType} {asyncMethod.Identifier.ToFullString()}";
                            if (!sigs.Contains(sig))
                            {
                                sigs.Add(sig);
                                Console.WriteLine(sig);
                            }
                        }
                    }
                }
            }
        }
    }

}
