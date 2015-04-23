using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net.Serialization;
using Nest.Resolvers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using NUnit.Framework;

namespace Nest.Tests.Integration.Search
{
    [TestFixture]
    public class SearchWithPolymorphismDocs
    {
        [Test]
        public void TestOnStudentPol()
        {
            var client = MakeElasticClient("polystudent");

            client.Delete<Student>(1);
            client.Delete<Student>(2);

            var instance0 = new Student
            {
                Id = 1,
                Name = "Name1",
                Size = 1,
                DataEncoded = "dlskfndlksfnkldsnfkl="
            };

            var instance1 = new StudentDev
            {
                Id = 2,
                Name = "Name2",
                Size = 2,
                DataEncoded = "dlskfndlksfnkldsnfkl=",
                University = "home"
            };

            var resp0 = client.Index(instance0);
            var resp1 = client.Index<Student>(instance1);

            Assert.True(resp0.Created);
            Assert.True(resp1.Created);
        }

        [Test]
        public void TestOnReadStudentsPol()
        {
            var client = MakeElasticClient("polystudent");
            var response = client.Search<Student>(descriptor => descriptor
                .From(0)
                .Size(2)
                );

            var res0 = client.Get<Student>(descriptor => descriptor.Id(1));
            Assert.True(res0.Found);

            var res1 = client.Get<Student>(descriptor => descriptor.Id(2));
            Assert.True(res1.Found);

            Assert.NotNull(response);

            // zero documents, if you see the [response.ServerError] you could understand the problem.
            Assert.AreEqual(2, response.Documents.Count());
        }

        /// <summary>
        /// Makes the elastic client (used for this test).
        /// </summary>
        /// <param name="defaultIndex">The default index.</param>
        /// <returns></returns>
        private static ElasticClient MakeElasticClient(string defaultIndex)
        {
            var settings = MakeSettings(defaultIndex)
                .ExposeRawResponse()
                .UsePrettyResponses()
                ;

            settings.SetJsonSerializerSettingsModifier(
                delegate(JsonSerializerSettings zz)
                {
                    zz.NullValueHandling = NullValueHandling.Ignore;
                    zz.MissingMemberHandling = MissingMemberHandling.Ignore;
                    zz.TypeNameHandling = TypeNameHandling.Objects;
                    zz.Binder = new CustomBinder(
                        new List<KeyValuePair<string, Type>>
                        {
                            new KeyValuePair<string, Type>("Student", typeof(Student)),
                            new KeyValuePair<string, Type>("StudentDev", typeof(StudentDev))
                        });

                    zz.ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor;
                    zz.ContractResolver = new DynamicContractResolver(settings);
                });
            return new ElasticClient(settings, null, new MoreThanNestSerializer(settings));
        }

        /// <summary>
        /// Makes the default client (not used for this test).
        /// </summary>
        /// <param name="defaultIndex">The default index.</param>
        /// <returns></returns>
        private static ElasticClient MakeDefaultClient(string defaultIndex)
        {
            var settings = MakeSettings(defaultIndex);
            return new ElasticClient(settings);
        }


        private static ConnectionSettings MakeSettings(string defaultIndex)
        {
            var uri = new Uri("http://localhost:9200");
            var settings = new ConnectionSettings(uri, defaultIndex);
            return settings;
        }
    }

    #region Contract resolver for serializing / deserializing dynamic members.
    public class DynamicContractResolver
        : ElasticContractResolver
    {
        private static readonly Type DynamicType;
        private readonly List<MemberInfo> members;

        /// <summary>
        /// 
        /// </summary>
        static DynamicContractResolver()
        {
            DynamicType = typeof(IDynamicMetaObjectProvider);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionSettings"></param>
        public DynamicContractResolver(IConnectionSettingsValues connectionSettings)
            : base(connectionSettings)
        {
            this.members = new List<MemberInfo>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="objectType"></param>
        /// <returns></returns>
        protected override JsonContract CreateContract(Type objectType)
        {
            var contract = base.CreateContract(objectType);

            if (!DynamicType.IsAssignableFrom(objectType))
                return contract;

            /*
            NOTE: this code serves for serializing dynamic members..
            */
            dynamic ctr = contract;
            foreach (var prop in ctr.Properties)
            {
                prop.HasMemberAttribute = true;
            }
            return contract;
        }

        protected override List<MemberInfo> GetSerializableMembers(Type objectType)
        {
            var origMembers = base.GetSerializableMembers(objectType);
            this.members.AddRange(origMembers);
            return origMembers;
        }

        protected override string ResolvePropertyName(string propertyName)
        {
            try
            {
                if (this.members.Any(info => info.Name.Equals(propertyName)))
                    return base.ResolvePropertyName(propertyName);

                return propertyName;
            }
            catch (Exception ex)
            {
                throw new Exception("ex", ex);
            }
        }

        /// <summary>
        /// Creates the property.
        /// </summary>
        /// <param name="member">The member.</param>
        /// <param name="memberSerialization">The member serialization.</param>
        /// <returns></returns>
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var property = base.CreateProperty(member, memberSerialization);
            if (!(member is PropertyInfo))
                return property;

            dynamic prp = member;
            property.Writable = prp.GetSetMethod(true) != null;
            property.Readable = prp.GetGetMethod(true) != null;

            return property;
        }
    }
    #endregion

    #region Binder for type names.
    public class CustomBinder
        : SerializationBinder
    {
        private Dictionary<string, Type> keyTypes;

        public CustomBinder(IEnumerable<KeyValuePair<string, Type>> binder)
        {
            this.keyTypes = new Dictionary<string, Type>();
            foreach (var keyValuePair in binder)
            {
                this.keyTypes.Add(keyValuePair.Key, keyValuePair.Value);
            }
        }

        public override Type BindToType(string assemblyName, string typeName)
        {
            var ret = this.keyTypes[typeName];
            return ret;
        }

        public override void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            assemblyName = null;
            typeName = serializedType.IsGenericType ? serializedType.FullName : serializedType.Name;
            if (!this.keyTypes.ContainsKey(typeName))
                this.keyTypes.Add(typeName, serializedType);
        }
    }
    #endregion

    #region POCOS
    public class Student
    {
        public Student()
        {
            this.Version = 1;
        }

        public int? Id { get; set; }

        public string Name { get; set; }


        public long Size { get; set; }


        public string DataEncoded { get; set; }


        public long? Version { get; private set; }


        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            if (obj.GetType() == this.GetType())
                return obj.GetHashCode() == this.GetHashCode();

            return false;
        }

        public override int GetHashCode()
        {
            return (this.Name.GetHashCode() - this.DataEncoded.GetHashCode()) * 7;
        }
    }

    public class StudentDev
        : Student
    {
        public string University { get; set; }
    }
    #endregion

    #region (SOLUTION) custom NestSerializer
    public class MoreThanNestSerializer
        : NestSerializer
    {
        //private readonly JsonSerializerSettings serializationSettings;
        //private readonly HashSet<Type> typesToInspect;

        public MoreThanNestSerializer(IConnectionSettingsValues connectionSettings, IEnumerable<Type> typesToInspect = null)
            : base(connectionSettings)
        {
            //this.typesToInspect = new HashSet<Type>(typesToInspect ?? Enumerable.Empty<Type>());
        }

        public override byte[] Serialize(object data, SerializationFormatting formatting = SerializationFormatting.Indented)
        {
            var format = formatting == SerializationFormatting.None ? Formatting.None : Formatting.Indented;

            if (data == null)
                return null;

            var ret = base.Serialize(data, formatting);
            
            string originalJson = Encoding.UTF8.GetString(ret);
            var jObject = JObject.Parse(originalJson);
            if (jObject.Remove("$type"))
                return Encoding.UTF8.GetBytes(jObject.ToString(format));

            return ret;
        }
    }
    #endregion
}
